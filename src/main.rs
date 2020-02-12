use std::process;
use std::error::Error;
use std::time::Duration;
use clap::{Arg, App};

#[macro_use]
extern crate lazy_static;

mod esdump;
mod compress;

use esdump::EsDump;

/**
 * @author qianwj
 * @since v0.1.0
 * es dump工具
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    esdump::dump(&app()).await
}

fn app() -> EsDump {
    let args = &[
        Arg::with_name("addr").help("default address: http://localhost:9200").empty_values(false).short("A"),
        Arg::with_name("index").help("dump index").empty_values(false).short("i"),
        Arg::with_name("scroll_window").help("default scroll_window: 1m").empty_values(false).short("w"),
        Arg::with_name("scroll_size").help("default scroll_size: 10000").empty_values(false).short("s"),
        Arg::with_name("dump_path").help("default dump path: ./esdump").empty_values(false).short("p"),
        Arg::with_name("query").help("query params").empty_values(false).short("q"),
        Arg::with_name("user").help("user").empty_values(false).short("U"),
        Arg::with_name("password").help("your es password").empty_values(false).short("P"),
        Arg::with_name("rt").help("http request timeout(s)").empty_values(false).short("rt"),
        Arg::with_name("ct").help("http connect timeout(s)").empty_values(false).short("ct"),
        Arg::with_name("midle").help("http max idle connection per host").empty_values(false).short("midle")
    ];
    let app = App::new("esdump")
        .version("0.1.0").author("qianwj<qwjlu@sina.com>").about("es dump tools, powered by qianwj")
        .args(args).get_matches();
    let index = match app.value_of("index") {
        None => {
            eprintln!("command must have index");
            process::exit(1);
        },
        Some(v) => v
    };
    let mut dump = EsDump::new(index);
    match app.value_of("addr") {
        None => (),
        Some(v) => dump = dump.addr(v)
    };
    match app.value_of("scroll_window") {
        None => (),
        Some(v) => dump = dump.scroll(v)
    };
    match app.value_of("scroll_size") {
        None => (),
        Some(v) => dump = dump.scroll_size(v.parse().unwrap())
    };
    match app.value_of("dump_path") {
        None => (),
        Some(v) => dump = dump.path(v)
    };
    match app.value_of("query") {
        None => (),
        Some(v) => dump = dump.query(v)
    };
    match app.value_of("user") {
        None => (),
        Some(v) => dump = dump.user(v)
    };
    match app.value_of("password") {
        None => (),
        Some(v) => dump = dump.password(v),
    };
    let req_timeout = match app.value_of("rt") {
        None => None,
        Some(v) => Some(Duration::from_secs(v.parse().unwrap()))
    };
    let conn_timeout = match app.value_of("ct") {
        None => None,
        Some(v) => Some(Duration::from_secs(v.parse().unwrap()))
    };
    let midle: Option<usize> = match app.value_of("midle") {
        None => None,
        Some(v) => Some(v.parse().unwrap())
    };
    dump.client(req_timeout, conn_timeout, midle)
}