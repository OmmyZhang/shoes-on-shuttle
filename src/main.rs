mod address;
mod async_stream;
mod buf_reader;
mod client_proxy_selector;
mod config;
mod copy_bidirectional;
mod copy_bidirectional_message;
mod copy_multidirectional_message;
mod http_handler;
mod hysteria2_server;
mod noop_stream;
mod option_util;
mod port_forward_handler;
mod quic_server;
mod quic_stream;
mod resolver;
mod rustls_util;
mod salt_checker;
mod shadow_tls;
mod shadowsocks;
mod snell;
mod socket_util;
mod socks_handler;
mod stream_reader;
mod tcp;
mod thread_util;
mod timed_salt_checker;
mod tls_handler;
mod trojan_handler;
mod tuic_server;
mod udp_message_stream;
mod udp_multi_message_stream;
mod util;
mod vless_handler;
mod vless_message_stream;
mod vmess;
mod websocket;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use log::debug;
use tcp_server::start_tcp_servers;
use tokio::task::JoinHandle;

use crate::address::NetLocation;
use crate::config::{BindLocation, ServerConfig, Transport};
use crate::quic_server::start_quic_servers;
use crate::thread_util::set_num_threads;
use tcp::*;

async fn start_servers(config: ServerConfig) -> std::io::Result<Vec<JoinHandle<()>>> {
    let mut join_handles = Vec::with_capacity(3);

    match config.transport {
        Transport::Tcp => match start_tcp_servers(config.clone()).await {
            Ok(handles) => {
                join_handles.extend(handles);
            }
            Err(e) => {
                for join_handle in join_handles {
                    join_handle.abort();
                }
                return Err(e);
            }
        },
        Transport::Quic => match start_quic_servers(config.clone()).await {
            Ok(handles) => {
                join_handles.extend(handles);
            }
            Err(e) => {
                for join_handle in join_handles {
                    join_handle.abort();
                }
                return Err(e);
            }
        },
        Transport::Udp => todo!(),
    }

    if join_handles.is_empty() {
        return Err(std::io::Error::other(format!(
            "failed to start servers at {}",
            &config.bind_location
        )));
    }

    Ok(join_handles)
}

pub struct ShoesService(pub ServerConfig);

#[shuttle_runtime::async_trait]
impl shuttle_runtime::Service for ShoesService {
    async fn bind(self, addr: std::net::SocketAddr) -> Result<(), shuttle_runtime::Error> {
        let config = self.0;

        let config = ServerConfig {
            bind_location: BindLocation::Address(
                NetLocation::from_ip_addr(addr.ip(), addr.port()).into(),
            ),
            ..config
        };

        let handles = start_servers(config).await?;
        for handle in handles {
            handle.await.map_err(shuttle_runtime::CustomError::new)?;
        }

        Ok(())
    }
}

#[shuttle_runtime::main]
async fn init() -> Result<ShoesService, shuttle_runtime::Error> {
    let num_threads = std::cmp::max(
        2,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    );
    debug!("Runtime threads: {}", num_threads);

    // Used by QUIC to figure out the number of endpoints.
    // TODO: can we pass it in instead?
    set_num_threads(num_threads);

    let configs = config::load_configs(&["config.shoes.yaml".to_owned()]).await?;
    let (configs, _) = config::validate_configs(configs).await?;
    let config = configs.into_iter().next().unwrap();
    Ok(ShoesService(config))
}
