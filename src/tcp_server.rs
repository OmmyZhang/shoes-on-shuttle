use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, warn};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::address::NetLocation;
use crate::async_stream::AsyncStream;
use crate::client_proxy_selector::{ClientProxySelector, ConnectDecision};
use crate::config::{BindLocation, ConfigSelection, ServerConfig, TcpConfig};
use crate::copy_bidirectional::copy_bidirectional;
use crate::copy_bidirectional_message::copy_bidirectional_message;
use crate::copy_multidirectional_message::copy_multidirectional_message;
use crate::resolver::{resolve_single_address, NativeResolver, Resolver};
use crate::tcp_client_connector::TcpClientConnector;
use crate::tcp_handler::{TcpServerHandler, TcpServerSetupResult};
use crate::tcp_handler_util::{create_tcp_client_proxy_selector, create_tcp_server_handler};
use crate::udp_direct_message_stream::UdpDirectMessageStream;

async fn run_tcp_server(
    bind_address: SocketAddr,
    tcp_config: TcpConfig,
    client_proxy_selector: Arc<ClientProxySelector<TcpClientConnector>>,
    server_handler: Arc<Box<dyn TcpServerHandler>>,
) -> std::io::Result<()> {
    let TcpConfig { no_delay } = tcp_config;

    let resolver: Arc<dyn Resolver> = Arc::new(NativeResolver::new());

    let listener = tokio::net::TcpListener::bind(bind_address).await.unwrap();

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("Accept failed: {}", e);
                continue;
            }
        };

        if no_delay {
            if let Err(e) = stream.set_nodelay(true) {
                error!("Failed to set TCP nodelay: {}", e);
            }
        }

        // TODO: allow this be to Option<Arc<ClientProxySelector<..>>> when
        // there are no rules or proxies specified.
        let cloned_provider = client_proxy_selector.clone();
        let cloned_cache = resolver.clone();
        let cloned_handler = server_handler.clone();
        tokio::spawn(async move {
            if let Err(e) =
                process_stream(stream, cloned_handler, cloned_provider, cloned_cache).await
            {
                error!("{}:{} finished with error: {:?}", addr.ip(), addr.port(), e);
            } else {
                debug!("{}:{} finished successfully", addr.ip(), addr.port());
            }
        });
    }
}

#[cfg(target_family = "unix")]
async fn run_unix_server(
    path_buf: PathBuf,
    client_proxy_selector: Arc<ClientProxySelector<TcpClientConnector>>,
    server_handler: Arc<Box<dyn TcpServerHandler>>,
) -> std::io::Result<()> {
    let resolver: Arc<dyn Resolver> = Arc::new(NativeResolver::new());

    if tokio::fs::symlink_metadata(&path_buf).await.is_ok() {
        println!(
            "WARNING: replacing file at socket path {}",
            path_buf.display()
        );
        let _ = tokio::fs::remove_file(&path_buf).await;
    }

    let listener = tokio::net::UnixListener::bind(path_buf).unwrap();

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("Accept failed: {:?}", e);
                continue;
            }
        };

        let cloned_provider = client_proxy_selector.clone();
        let cloned_cache = resolver.clone();
        let cloned_handler = server_handler.clone();
        tokio::spawn(async move {
            if let Err(e) =
                process_stream(stream, cloned_handler, cloned_provider, cloned_cache).await
            {
                error!("{:?} finished with error: {:?}", addr, e);
            } else {
                debug!("{:?} finished successfully", addr);
            }
        });
    }
}

async fn setup_server_stream<AS>(
    stream: AS,
    server_handler: Arc<Box<dyn TcpServerHandler>>,
) -> std::io::Result<TcpServerSetupResult>
where
    AS: AsyncStream + 'static,
{
    let server_stream = Box::new(stream);
    server_handler.setup_server_stream(server_stream).await
}

async fn process_stream<AS>(
    stream: AS,
    server_handler: Arc<Box<dyn TcpServerHandler>>,
    client_proxy_selector: Arc<ClientProxySelector<TcpClientConnector>>,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()>
where
    AS: AsyncStream + 'static,
{
    let setup_server_stream_future = timeout(
        Duration::from_secs(60),
        setup_server_stream(stream, server_handler),
    );

    let setup_result = match setup_server_stream_future.await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            return Err(std::io::Error::new(
                e.kind(),
                format!("failed to setup server stream: {}", e),
            ));
        }
        Err(elapsed) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("server setup timed out: {}", elapsed),
            ));
        }
    };

    match setup_result {
        TcpServerSetupResult::TcpForward {
            remote_location,
            stream: mut server_stream,
            need_initial_flush: server_need_initial_flush,
            override_proxy_provider,
            connection_success_response,
            initial_remote_data,
        } => {
            let selected_proxy_provider = if override_proxy_provider.is_one() {
                override_proxy_provider.unwrap()
            } else {
                client_proxy_selector
            };

            let setup_client_stream_future = timeout(
                Duration::from_secs(60),
                setup_client_stream(
                    &mut server_stream,
                    selected_proxy_provider,
                    resolver,
                    remote_location.clone(),
                ),
            );

            let mut client_stream = match setup_client_stream_future.await {
                Ok(Ok(Some(s))) => s,
                Ok(Ok(None)) => {
                    // Must have been blocked.
                    let _ = server_stream.shutdown().await;
                    return Ok(());
                }
                Ok(Err(e)) => {
                    let _ = server_stream.shutdown().await;
                    return Err(std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to setup client stream to {}: {}",
                            remote_location, e
                        ),
                    ));
                }
                Err(elapsed) => {
                    let _ = server_stream.shutdown().await;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("client setup to {} timed out: {}", remote_location, elapsed),
                    ));
                }
            };

            if let Some(data) = connection_success_response {
                server_stream.write_all(&data).await?;
                // server_need_initial_flush should be set to true by the handler if
                // it's needed.
            }

            let client_need_initial_flush = match initial_remote_data {
                Some(data) => {
                    client_stream.write_all(&data).await?;
                    true
                }
                None => false,
            };

            let copy_result = copy_bidirectional(
                &mut server_stream,
                &mut client_stream,
                server_need_initial_flush,
                client_need_initial_flush,
            )
            .await;

            let (_, _) = futures::join!(server_stream.shutdown(), client_stream.shutdown());

            copy_result?;
            Ok(())
        }
        TcpServerSetupResult::BidirectionalUdpForward {
            remote_location,
            stream: mut server_stream,
        } => {
            let action = client_proxy_selector
                .judge(remote_location, &resolver)
                .await?;
            match action {
                ConnectDecision::Allow {
                    client_proxy,
                    remote_location,
                } => {
                    let remote_addr = resolve_single_address(&resolver, &remote_location).await?;
                    let client_socket = client_proxy.configure_udp_socket()?;
                    client_socket.connect(remote_addr).await?;

                    let mut client_socket = Box::new(client_socket);

                    let copy_result =
                        copy_bidirectional_message(&mut server_stream, &mut client_socket).await;

                    // TODO: add async trait ext and make this work
                    //let (_, _) = futures::join!(server_stream.shutdown_message(), client_stream.shutdown_message());

                    copy_result?;
                    Ok(())
                }
                ConnectDecision::Block => {
                    // Must have been blocked.
                    // TODO: add async trait ext and make this work
                    // let _ = server_stream.shutdown_message().await;
                    Ok(())
                }
            }
        }
        TcpServerSetupResult::MultidirectionalUdpForward {
            stream: mut server_stream,
            need_initial_flush: server_need_initial_flush,
        } => {
            let action = client_proxy_selector.default_decision();
            match action {
                ConnectDecision::Allow {
                    client_proxy,
                    remote_location: _,
                } => {
                    let client_socket = client_proxy.configure_udp_socket()?;
                    let mut client_stream =
                        Box::new(UdpDirectMessageStream::new(client_socket, resolver));

                    let copy_result = copy_multidirectional_message(
                        &mut server_stream,
                        &mut client_stream,
                        server_need_initial_flush,
                        false,
                    )
                    .await;

                    // TODO: add async trait ext and make this work
                    //let (_, _) = futures::join!(server_stream.shutdown_message(), client_stream.shutdown_message());

                    copy_result?;
                    Ok(())
                }
                ConnectDecision::Block => {
                    warn!("Blocked multidirectional udp forward, because the default action is to block.");
                    // TODO: add async trait ext and make this work
                    // let _ = server_stream.shutdown_message().await;
                    Ok(())
                }
            }
        }
    }
}

pub async fn setup_client_stream(
    server_stream: &mut Box<dyn AsyncStream>,
    client_proxy_selector: Arc<ClientProxySelector<TcpClientConnector>>,
    resolver: Arc<dyn Resolver>,
    remote_location: NetLocation,
) -> std::io::Result<Option<Box<dyn AsyncStream>>> {
    let action = client_proxy_selector
        .judge(remote_location, &resolver)
        .await?;

    match action {
        ConnectDecision::Allow {
            client_proxy,
            remote_location,
        } => {
            let client_stream = client_proxy
                .connect(server_stream, remote_location, &resolver)
                .await?;
            Ok(Some(client_stream))
        }
        ConnectDecision::Block => Ok(None),
    }
}

pub async fn start_tcp_server(config: ServerConfig) -> std::io::Result<JoinHandle<()>> {
    let ServerConfig {
        bind_location,
        tcp_settings,
        protocol,
        rules,
        ..
    } = config;

    println!("Starting {} TCP server at {}", &protocol, &bind_location);

    let rules = rules.map(ConfigSelection::unwrap_config).into_vec();
    // We should always have a direct entry.
    assert!(!rules.is_empty());

    let tcp_config = tcp_settings.unwrap_or_else(TcpConfig::default);

    let client_proxy_selector = Arc::new(create_tcp_client_proxy_selector(rules.clone()));

    let mut rules_stack = vec![rules];
    let tcp_handler: Arc<Box<dyn TcpServerHandler>> =
        Arc::new(create_tcp_server_handler(protocol, &mut rules_stack));
    debug!("TCP handler: {:?}", tcp_handler);

    Ok(tokio::spawn(async move {
        match bind_location {
            BindLocation::Address(a) => {
                // TODO: make this non-blocking?
                let socket_addr = a.to_socket_addr().unwrap();
                run_tcp_server(socket_addr, tcp_config, client_proxy_selector, tcp_handler)
                    .await
                    .unwrap();
            }
            BindLocation::Path(path_buf) => {
                #[cfg(target_family = "unix")]
                {
                    run_unix_server(path_buf, client_proxy_selector, tcp_handler)
                        .await
                        .unwrap();
                }
                #[cfg(not(target_family = "unix"))]
                {
                    panic!("Unix sockets are not supported on non-unix OSes.");
                }
            }
        }
    }))
}
