// #[macro_use] extern crate diesel;
// extern crate dotenv;
extern crate crypto_hash;
extern crate num;
extern crate postgres;
extern crate num_cpus;

#[warn(unused_imports)]
use postgres::{Connection, TlsMode};
use crypto_hash::{Algorithm, hex_digest};
// use std::collections::HashMap;
// use std::sync::{Arc, Mutex};
// use num::bigint::BigInt;
use std::thread;
// use diesel::prelude::*;
// use diesel::pg::PgConnection;
// use dotenv::dotenv;
// use std::env;

// pub fn establish_connection() -> PgConnection {
// dotenv().ok();
//
// let database_url = env::var("DATABASE_URL")
// .expect("DATABASE_URL must be set");
// PgConnection::establish(&database_url)
// .expect(&format!("Error connecting to {}", database_url))
// }
//

// #[derive(Queryable)]
// pub struct Mapo {
//    pub bits: isize,
//    pub val: BigInt,
//    pub count: isize,
// }


struct Splitter {
    len: usize,
    val: u32,
}

fn split_by_bits(lens: &Vec<usize>,
                 sha: String /* , ac: &Fn(&String, usize) */)
                 -> Vec<Splitter> {
    let mut ret = Vec::new();
    for i in lens.iter() {
        let len = sha.len() / i;
        let mut j = 0;
        while j < sha.len() {
            let sub: String = sha.chars().skip(j).take(len).collect();
            ret.push(Splitter {
                len: len,
                val: u32::from_str_radix(&sub, 16).unwrap(),
            });
            // ac(&sub, j);
            j = j + len;
        }
    }
    return ret;
}

fn transform_u32_to_array_of_u8(x: u32) -> [u8; 4] {
    let b1: u8 = ((x >> 24) & 0xff) as u8;
    let b2: u8 = ((x >> 16) & 0xff) as u8;
    let b3: u8 = ((x >> 8) & 0xff) as u8;
    let b4: u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4];
}

struct Mapo {
    pub len: usize,
    // pub lenMap : [Option<HashMap<u32, LenValCnt>>]
    pub conn: postgres::Connection,
    pub buffer: String,
    pub buff_items: usize,
    pub elements: usize,
    pub prefix: String,
}

impl Mapo {
    fn create_mapo(&self) {
        if self.conn
            .execute(&format!("CREATE TABLE mapo{} (
                val              INTEGER \
                               NOT NULL
            )", self.len), &[])
            .is_ok() {
            println!("create mapo {}", self.len);
        } else {
            println!("failed create mapo {}", self.len);
        }
    }

    fn new(len: usize, elements: usize) -> Mapo {
        let prefix = format!("INSERT INTO mapo{} values", len);
        let ret = Mapo {
            conn: Connection::connect("postgresql://root:meno@localhost:5433/root", TlsMode::None)
                .unwrap(),
            len: len,
            prefix: prefix.clone(),
            buffer: prefix.clone(),
            buff_items: 0,
            elements: elements,
        };
        ret.create_mapo();
        return ret;
    }

    fn add(&mut self, val: u32) {
        self.buffer.push_str(&format!("({})", val));
        if self.buff_items < self.elements {
            self.buff_items += 1;
        } else {
            self.commit();
        }
    }

    // fn for_each(&self, len: usize, ac: &Fn(usize, u32, u32)) {
    // for i in self.lenMap[len].values() {
    // ac(i.len, i.val, i.count);
    // }
    // }
    //
    fn commit(&mut self) {
        self.conn.execute(&self.buffer, &[]).unwrap();
        self.buff_items = 0;
        self.buffer.clear();
        self.buffer.push_str(&self.prefix);
    }
}

fn main() {
    // infer_schema!("dotenv:DATABASE_URL");
    // let start = 0;
    let cores = num_cpus::get();
    let step = (4294967296 as usize) / (cores as usize);
    let mut threads = Vec::new();
    let lens = vec![8];
    // let so: HashMap<usize, HashMap<u32, usize>> = HashMap::new();
    // let res_so = Arc::new(Mutex:jk:new(so));
    // create_mapos(&conn, lens);
    // let tres_so = res_so.clone();
    for yy in 0..cores {
        let y = yy.clone();
        let my_lens = lens.clone();
        threads.push(thread::spawn(move || {
            println!("Range for:{}:{:08x}-{:08x}",
                     y,
                     (step * y),
                     (step * (y + 1)));
            // let mut my: HashMap<String, isize> = HashMap::new();
            let mut my = Mapo::new(8, 10000);
            for x in (step * y)..(step * (y + 1)) {
                // let data = format!("{}", x);
                let result = hex_digest(Algorithm::SHA256, &transform_u32_to_array_of_u8(x as u32));
                // println!("{}", result)
                // split_by_bits(result, &mut|val, bits| {
                for val in split_by_bits(&my_lens, result) {
                    my.add(val.val);
                }
            }
            my.commit();
        }));
    }
    while threads.len() > 0 {
        let first = threads.pop().unwrap();
        first.join();
    }
}

// fn dump_to_sql(conn: &postgres::Connection,
// res_so: &Arc<std::sync::Mutex<HashMap<usize, HashMap<u32, usize>>>>) {
// let mut x = 0;
// let mut trans = conn.transaction().unwrap();
// let mut sql = String::from("INSERT INTO mapo values ");
// let mut comma = "";
// for (val, cnt) in res_so.lock().unwrap().iter() {
// let v = BigInt::parse_bytes(val.as_bytes(), 16).unwrap();
// sql = sql + &format!("{}({}, {}, {}) ", comma, val.len(), v.to_str_radix(10), *cnt);
// comma = ",";
// if (x % 6000) == 0 {
// println!("sql:{}", x);
// conn.execute(&sql, &[]).unwrap();
// sql = String::from("INSERT INTO mapo values ");
// comma = "";
// trans.commit();
// trans = conn.transaction().unwrap();
// }
// x = x + 1
// }
// conn.execute(&sql, &[]).unwrap();
// trans.commit();
// }
//
// struct LenValCnt {
// pub len : u8,
// pub val : u32,
// pub cnt : u32
// }
//
