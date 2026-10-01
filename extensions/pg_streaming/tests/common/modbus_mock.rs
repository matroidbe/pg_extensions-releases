//! In-process Modbus TCP mock server for integration tests — a canned
//! register map that answers reads and records writes. Mirrors the
//! unit-test mock inside the crate (test cfg items aren't visible to
//! integration tests).

use std::collections::HashMap;
use std::future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio_modbus::server::tcp::{accept_tcp_connection, Server};
use tokio_modbus::server::Service;
use tokio_modbus::{ExceptionCode, Request, Response};

#[derive(Debug, Clone, Default)]
pub struct MockDevice {
    pub holding: Arc<Mutex<HashMap<u16, u16>>>,
    pub input: Arc<Mutex<HashMap<u16, u16>>>,
    pub coils: Arc<Mutex<HashMap<u16, bool>>>,
    pub discrete: Arc<Mutex<HashMap<u16, bool>>>,
}

impl MockDevice {
    pub fn set_holding(&self, addr: u16, words: &[u16]) {
        let mut map = self.holding.lock().unwrap();
        for (i, w) in words.iter().enumerate() {
            map.insert(addr + i as u16, *w);
        }
    }

    pub fn set_coil(&self, addr: u16, value: bool) {
        self.coils.lock().unwrap().insert(addr, value);
    }

    pub fn holding_at(&self, addr: u16) -> u16 {
        *self.holding.lock().unwrap().get(&addr).unwrap_or(&0)
    }

    pub fn coil_at(&self, addr: u16) -> bool {
        *self.coils.lock().unwrap().get(&addr).unwrap_or(&false)
    }

    fn read_words(map: &Mutex<HashMap<u16, u16>>, addr: u16, cnt: u16) -> Vec<u16> {
        let map = map.lock().unwrap();
        (0..cnt)
            .map(|i| *map.get(&(addr + i)).unwrap_or(&0))
            .collect()
    }

    fn read_bits(map: &Mutex<HashMap<u16, bool>>, addr: u16, cnt: u16) -> Vec<bool> {
        let map = map.lock().unwrap();
        (0..cnt)
            .map(|i| *map.get(&(addr + i)).unwrap_or(&false))
            .collect()
    }
}

impl Service for MockDevice {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = future::Ready<Result<Self::Response, Self::Exception>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let response = match req {
            Request::ReadHoldingRegisters(addr, cnt) => Ok(Response::ReadHoldingRegisters(
                Self::read_words(&self.holding, addr, cnt),
            )),
            Request::ReadInputRegisters(addr, cnt) => Ok(Response::ReadInputRegisters(
                Self::read_words(&self.input, addr, cnt),
            )),
            Request::ReadCoils(addr, cnt) => {
                Ok(Response::ReadCoils(Self::read_bits(&self.coils, addr, cnt)))
            }
            Request::ReadDiscreteInputs(addr, cnt) => Ok(Response::ReadDiscreteInputs(
                Self::read_bits(&self.discrete, addr, cnt),
            )),
            Request::WriteSingleCoil(addr, value) => {
                self.coils.lock().unwrap().insert(addr, value);
                Ok(Response::WriteSingleCoil(addr, value))
            }
            Request::WriteSingleRegister(addr, word) => {
                self.holding.lock().unwrap().insert(addr, word);
                Ok(Response::WriteSingleRegister(addr, word))
            }
            Request::WriteMultipleRegisters(addr, words) => {
                let cnt = words.len() as u16;
                self.set_holding(addr, &words);
                Ok(Response::WriteMultipleRegisters(addr, cnt))
            }
            _ => Err(ExceptionCode::IllegalFunction),
        };
        future::ready(response)
    }
}

/// Spawn the mock server on a fixed localhost port inside `rt`. The PG
/// background workers reach it via 127.0.0.1 (test process and server
/// share the host). Fixed uncommon ports avoid clashing with parallel
/// services; tests run with --test-threads=1.
pub fn spawn_on(rt: &tokio::runtime::Runtime, device: MockDevice, port: u16) -> SocketAddr {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let listener = rt.block_on(async {
        tokio::net::TcpListener::bind(addr)
            .await
            .unwrap_or_else(|e| panic!("bind mock modbus server on {}: {}", addr, e))
    });
    rt.spawn(async move {
        let server = Server::new(listener);
        let on_connected = |stream, socket_addr| {
            let device = device.clone();
            async move {
                accept_tcp_connection(stream, socket_addr, move |_| Ok(Some(device.clone())))
            }
        };
        let _ = server.serve(&on_connected, |_err| {}).await;
    });
    addr
}
