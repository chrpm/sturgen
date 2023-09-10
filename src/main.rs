use clap::Parser;
use log::{debug, info};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Cli {
    /// The path to the db dir
    #[arg(short, long)]
    db: PathBuf,
}

fn main() {
    env_logger::init();

    let args = Cli::parse();
    let data_store_name = args.db;
    info!("starting db named {}", data_store_name.display());

    let mut ds = open_data_store(data_store_name.clone()).expect("failed to open db");

    ds.get("test1".to_string());
    ds.insert("test1".to_string(), "test2".to_string());
    ds.get("test1".to_string());
    ds.remove("test1".to_string());
    ds.get("test1".to_string());

    write_data_store_to_disk(ds).expect("failed to write data to disk");

    info!("shutting down");
}

const DATA_FILE_NAME: &str = "data";

fn get_data_file_path(data_store_path: &PathBuf) -> PathBuf {
    let mut dfp = data_store_path.clone();
    dfp.push(DATA_FILE_NAME);
    return dfp;
}

fn load_data_file(file_path: &Path, data_map: &mut HashMap<String, String>) {
    if let Ok(lines) = read_lines(file_path) {
        for line in lines {
            if let Ok(ip) = line {
                let r = raw_line_to_key_val(ip);
                data_map.insert(r.0, r.1);
            }
        }
    }
}

fn raw_line_to_key_val(raw: String) -> (String, String) {
    let parts: Vec<&str> = raw.split("||").collect();
    let key_raw = parts[0];
    let key = unescape_str(key_raw);
    let val_raw = parts[1];
    let val = unescape_str(val_raw);
    return (key.to_string(), val.to_string());
}

fn key_val_to_raw_line(key: &str, val: &str) -> String {
    let escaped_key = escape_str(key);
    let escaped_val = escape_str(val);
    return escaped_key + "||" + &escaped_val;
}

fn escape_str(s: &str) -> String {
    let s1 = str::replace(s, "\\", "\\\\");
    let s2 = str::replace(&s1, "|", "\\|");
    return s2;
}

fn unescape_str(raw: &str) -> String {
    let s1 = str::replace(&raw, "\\|", "|");
    let s2 = str::replace(&s1, "\\\\", "\\");
    return s2;
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn write_data_file(file_path: &Path, data_map: HashMap<String, String>) -> io::Result<()> {
    if file_path.is_file() {
        fs::remove_file(file_path)?;
    }

    let mut output = File::create(file_path)?;
    for (k, v) in data_map {
        let line = key_val_to_raw_line(&k, &v);
        writeln!(output, "{}", line)?
    }

    return Ok(());
}

struct DataStore {
    path: PathBuf,
    in_mem_data: HashMap<String, String>,
}

impl DataStore {
    pub fn insert(&mut self, key: String, val: String) {
        debug!("inserting key:{} val:{}", key, val);
        self.in_mem_data.insert(key, val);
    }

    pub fn get(&mut self, key: String) -> Option<&String> {
        debug!("getting key:{}", key);
        return self.in_mem_data.get(&key);
    }

    pub fn remove(&mut self, key: String) {
        debug!("removing key:{}", key);
        self.in_mem_data.remove(&key);
    }
}

fn open_data_store(data_store_name: PathBuf) -> Result<DataStore, String> {
    let mut in_mem = HashMap::new();

    if !data_store_name.is_dir() {
        return Err(String::from("directory doesn't exist"));
    }

    let file_name = get_data_file_path(&data_store_name);

    load_data_file(&file_name, &mut in_mem);

    return Ok(DataStore {
        path: data_store_name,
        in_mem_data: in_mem,
    });
}

fn write_data_store_to_disk(data_store: DataStore) -> Result<(), io::Error> {
    return write_data_file(
        &get_data_file_path(&data_store.path),
        data_store.in_mem_data,
    );
}
