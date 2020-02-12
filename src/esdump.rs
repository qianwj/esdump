use std::time::Duration;
use std::error::Error;
use std::io::Write;
use std::fs::File;
use std::sync::{Arc, atomic::{AtomicI32, Ordering}};
use std::collections::HashMap;

use reqwest::Client;
use serde_json::{Value, to_string};

lazy_static! {
    static ref COUNTER: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));
}

/**
 * @author qianwj
 * @since 0.1.1
 */
#[derive(Debug)]
pub struct EsDump {
    addr: String,
    index: String,
    scroll: String,
    scroll_size: i64,
    scroll_id: String,
    params: String,
    path: String,
    user: String,
    password: Option<String>,
    client: Client,
}

impl Clone for EsDump {

    fn clone (&self) -> Self {
        EsDump {
            addr: self.addr.clone(),
            index: self.index.clone(),
            scroll_size: self.scroll_size.clone(),
            scroll: self.scroll.clone(),
            scroll_id: self.scroll_id.clone(),
            params: self.params.clone(),
            path: self.path.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            client: self.client.clone()
        }
    }

}

impl EsDump {

    pub fn new(index: &str) -> Self {
        EsDump {
            addr: String::from("http://localhost:9200"),
            index: index.to_string(),
            scroll: String::from("1m"),
            scroll_size: 50000,
            scroll_id: String::new(),
            params: String::new(),
            path: String::from("./esdump"),
            user: String::new(),
            password: None,
            client: Client::default(),
        }
    }

    pub fn addr(mut self, addr: &str) -> EsDump {
        self.addr = addr.to_string();
        self
    }

    pub fn scroll(mut self, scroll: &str) -> EsDump {
        self.scroll = scroll.to_string();
        self
    }

    pub fn scroll_size(mut self, scroll_size: i64) -> EsDump {
        self.scroll_size = scroll_size;
        self
    }

    pub fn query(mut self, query: &str) -> EsDump {
        self.params = query.to_string();
        self
    }

    pub fn path(mut self, path: &str) -> EsDump {
        self.path = path.to_string();
        self
    }

    pub fn user(mut self, user: &str) -> EsDump {
        self.user = user.to_string();
        self
    }

    pub fn password(mut self, password: &str) -> EsDump {
        self.password = Some(password.to_string());
        self
    }

    pub fn client(
        mut self,
        req_timeout: Option<Duration>, 
        conn_timeout: Option<Duration>,
        max_idle_conn: Option<usize>
    ) -> EsDump {
        let mut builder = reqwest::ClientBuilder::default();
    
        match req_timeout {
            None => (),
            Some(timeout) => {
                builder = builder.timeout(timeout);
            }
        }
    
        match conn_timeout {
            None => (),
            Some(timeout) => {
                builder = builder.connect_timeout(timeout);
            }
        }
        
        match max_idle_conn {
            None => (),
            Some(idle) => {
                builder = builder.max_idle_per_host(idle);
            }
        }
    
        self.client = match builder.build() {
            Err(e) => {
                eprintln!("Constructing Reqwest Http Client failed, {}", e);
                Client::default()
            },
            Ok(client) => client
        };
        self
    }
    
    
}

pub async fn dump(dump: &EsDump) -> Result<(), Box<dyn Error>> {
    let mut copy = dump.clone();
    let user = dump.user.clone();
    let password = dump.password.clone();
    let params = dump.params.clone();

    let url = 
        format!("{addr}/{idx}/_search?scroll={scroll}&size={size}", addr = dump.addr, idx = dump.index, scroll = dump.scroll, size = dump.scroll_size);
    let response = dump.client
        .get(url.as_str())
        .basic_auth(user, password)
        .body(params).send().await?.json::<Value>().await?;

    let index_exists = match &response["error"]["reason"].as_str() {
        Some(_v) => {
            println!("no such index!");
            false
        },
        None => true
    };

    if !index_exists { return Ok(()) }

    let scroll_id = &response["_scroll_id"].as_str().unwrap();
    copy.scroll_id = scroll_id.to_string();


    let total_val = &response["hits"]["total"].as_i64();
    let total = match total_val {
        None => return Ok(()),
        Some(v) => {
            if *v <= (0 as i64) { return Ok(()); }
            v
        }
    };

    

    let data = &response["hits"]["hits"].as_array().unwrap();

    println!("scroll task start! scroll_id: {}, total: {}", scroll_id, total);

    // compute the sub tasks count
    let mut rest = *total - (data.len() as i64);
    let mut id = 1;
    let max_id = loop {
        rest -= dump.scroll_size;
        if rest <= 0 {
            break id;
        }
        id += 1;
    };
    id = 1;

    // create files
    match std::fs::create_dir(&dump.path) {
        Ok(()) => (),
        Err(_e) => ()
    }
    let file_name = format!("{path}/{idx}_{i}.data", path = dump.path, idx = dump.index, i = 0);
    let zip_file_name = format!("{path}/{idx}.zip", path = dump.path, idx = dump.index);

    // write first task data
    write_to_file(data, file_name.as_str(), dump.path.as_str(), zip_file_name.as_str(), &max_id)?;
    println!("esdump: {} sub task 0 complete", scroll_id);
    if max_id == 1 {
        crate::compress::zip(dump.path.as_str(), &zip_file_name);
        return Ok(());
    }
    
    // scroll others
    let url: String = format!("{addr}/_search/scroll", addr = dump.addr);
    
    while id <= max_id {
        let result = scroll_other(copy.clone(), &copy.client, url.as_str(), dump.path.as_str(), &id, &max_id).await;
        match result {
            Ok(()) => println!("id-{}: sub task finished", id),
            Err(e) => eprintln!("id-{}: sub task occur error: {}", id, e),
        }
        id += 1;
    }

    Ok(())
}

async fn scroll_other(dump: EsDump, client: &Client, url: &str, path: &str, id: &i32, max_id: &i32) -> Result<(), Box<dyn Error>> {
    let mut params = HashMap::new();
    params.insert("scroll_id", dump.scroll_id);
    params.insert("scroll", dump.scroll);
    let resp = client
        .get(url)
        .basic_auth(dump.user, dump.password).json(&params)
        .send().await?.json::<Value>().await?;
    match &resp["hits"]["hits"].as_array() {
        None => return Ok(()),
        Some(data) => {
            if data.len() > 0 {
                let file_name = format!("{path}/{idx}_{i}.data", path = path, idx = dump.index, i = id);
                let zip_file_name = format!("{path}/{idx}.zip", path = path, idx = dump.index);
                write_to_file(data, file_name.as_str(), path, zip_file_name.as_str(), max_id)?;
            }
            return Ok(())
        },
    };
}

fn write_to_file (data: &Vec<Value>, file_name: &str, file_path: &str, zip_file_name: &str, max_id: &i32) -> std::io::Result<()> {
    let mut file = File::create(file_name).expect(format!("failed create file: {}", file_name).as_str());
    for row in data.iter() { // 逐行读取数据
        let mut line = match to_string(row) {
            Err(_) => String::new(), 
            Ok(v) => v,
        };
        if line.len() == 0 { continue; }
        line.push('\n');
        file.write_all(line.as_bytes())?;
    }

    let current_id = (*COUNTER).fetch_add(1, Ordering::SeqCst);
    if current_id == *max_id { // 压缩开始
        crate::compress::zip(file_path, zip_file_name);
        (*COUNTER).store(0, Ordering::Release); // 计数器归0
    }
    // 压缩结束
    Ok(())
}