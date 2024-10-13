use anyhow::Result;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use kafka_starter_rust::kafka_protocol::Request;
use kafka_starter_rust::process::build_response;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("accepted new connection from: {}", addr);
                
                tokio::spawn(async move {
                    // Handle the connection here
                    match handle_connection(socket).await {
                        Ok(_) => println!("connection closed"),
                        Err(e) => println!("error: {}", e),
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(mut socket: tokio::net::TcpStream) -> Result<()> {
    let mut buffer = [0; 1024];

    loop {
        let read_bytes = socket.read(&mut buffer).await?;
        let request = Request::try_from_message(buffer[..read_bytes].to_vec())?;
        eprintln!("Request: {request:?}");
        let response = build_response(request)?;
        eprintln!("Response: {response:?}");
        let write_bytes = response.to_message();
        eprintln!("Raw response: {write_bytes:?}");
        socket.write_all(&write_bytes).await?;
    }
}
