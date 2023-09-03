use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;

fn main() {
    // todo: get from user
    let data_store_name = "starting";

    let mut ds = open_data_store(data_store_name);

    ds.insert("test1".to_string(), "test2".to_string());

    write_data_store_to_disk(ds);

    println!("Success");
}

const DATA_FILE_NAME: &str = "data";

fn data_file_name(data_store_name: &str) -> String {
    let file_name = format!("{}/{}", data_store_name, DATA_FILE_NAME);
    return file_name;
}

fn load_data_file(file_name: String,data_map: &mut HashMap<String, String>) {
    if let Ok(lines) = read_lines(file_name) {
        for line in lines {
            if let Ok(ip) = line {
                let r = raw_line_to_key_val(ip);
                data_map.insert(r.0,r.1);
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
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn write_data_file(data_store_name: String, data_map: HashMap<String, String>) -> std::io::Result<()> {
    let file_name = data_file_name(&data_store_name);

    let r = fs::remove_file(file_name.clone());
    r.ok(); // todo: add error handling

    if !std::path::Path::new(&data_store_name).exists() {
        std::fs::create_dir(data_store_name)?;
    }

    let mut output = File::create(file_name)?;
    for (k, v) in data_map {
        let line = key_val_to_raw_line(&k, &v);
        writeln!(output, "{}", line)?
    }
    return Ok(());
}

struct DataStore {
    name: String,
    in_mem_data: HashMap<String,String>
}

impl DataStore {
    pub fn insert(&mut self, key: String, val: String) {
        self.in_mem_data.insert(key, val);
    }

    pub fn get(&mut self, key: String) -> Option<&String> {
        return self.in_mem_data.get(&key);
    }

    pub fn remove(&mut self, key: String) {
        self.in_mem_data.remove(&key);
    }
}


fn open_data_store(data_store_name: &str) -> DataStore {
    let mut in_mem = HashMap::new();

    let file_name = data_file_name(data_store_name);

    load_data_file(file_name, &mut in_mem);

    return DataStore{
        name: data_store_name.to_string(),
        in_mem_data: in_mem 
    };
}

fn write_data_store_to_disk(data_store: DataStore) {
    let r = write_data_file(data_store.name, data_store.in_mem_data);
    r.unwrap()
}
