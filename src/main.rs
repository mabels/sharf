// #[macro_use] extern crate diesel;
//extern crate dotenv;
extern crate crypto_hash;
extern crate num;
extern crate postgres;

#[warn(unused_imports)]
use postgres::{Connection, TlsMode};
use crypto_hash::{Algorithm, hex_digest};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use num::bigint::BigInt;
use std::thread;
//use diesel::prelude::*;
//use diesel::pg::PgConnection;
//use dotenv::dotenv;
//use std::env;

/*
pub fn establish_connection() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    PgConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url))
}
*/

//#[derive(Queryable)]
//pub struct Mapo {
//    pub bits: isize,
//    pub val: BigInt,
//    pub count: isize,
//}


struct Splitter {
	len : usize,
	val : u32 
}

fn split_by_bits(sha: String/*, ac: &Fn(&String, usize)*/) -> Vec<Splitter> {
    let mut ret = Vec::new();
    for i in [32,16, 8].iter() {
        let len = sha.len()/i;
        let mut j = 0;
        while j < sha.len() {
            let sub : String = sha.chars().skip(j).take(len).collect();
	    ret.push(Splitter { len: len, val: u32::from_str_radix(&sub, 16).unwrap() });
            //ac(&sub, j);
            j = j + len;
        }
    }
    return ret;
}

/*
pub fn create_post<'a>(conn: &PgConnection, title: &'a str, body: &'a str) -> Post {
    use schema::posts;

    let new_post = NewPost {
        title: title,
        body: body,
    };

    diesel::insert(&new_post).into(posts::table)
        .get_result(conn)
        .expect("Error saving new post")
}
*/

fn transform_u32_to_array_of_u8(x:u32) -> [u8;4] {
    let b1 : u8 = ((x >> 24) & 0xff) as u8;
    let b2 : u8 = ((x >> 16) & 0xff) as u8;
    let b3 : u8 = ((x >> 8) & 0xff) as u8;
    let b4 : u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4]
}

fn create_mapos(conn: &postgres::Connection, v: &Vec<usize>) {
  for x in v {
    if conn.execute(&format!("CREATE TABLE mapo{} (
                    val              DECIMAL NOT NULL,
                    count            INTEGER)", x), &[]).is_ok() {
       println!("create mapo {}", x);
    } else {
       println!("failed create mapo {}", x);
    }
  }
}
fn dump_to_sql(conn: &postgres::Connection, res_so: &Arc<std::sync::Mutex<HashMap<usize, HashMap<u32, usize>>>>) {
        let mut x = 0;
        let mut trans = conn.transaction().unwrap();
        let mut sql = String::from("INSERT INTO mapo values ");
        let mut comma = "";
        for (val, cnt) in res_so.lock().unwrap().iter() {
            let v = BigInt::parse_bytes(val.as_bytes(), 16).unwrap();
            sql = sql + &format!("{}({}, {}, {}) ", comma, val.len(), v.to_str_radix(10), *cnt); 
            comma = ",";
            if (x % 6000) == 0 {
                println!("sql:{}", x);
                conn.execute(&sql, &[]).unwrap();
                sql = String::from("INSERT INTO mapo values ");
                comma = "";
                trans.commit();
                trans = conn.transaction().unwrap();
            }
            x = x + 1
        }
        conn.execute(&sql, &[]).unwrap();
        trans.commit();
}

struct LenValCnt {
	pub len : u8,
	pub val : u32,
	pub cnt : u32
}

struct Mapo {
	pub lens: [usize];
	pub lenMap : [Option<HashMap<u32, LenValCnt>>]
}

impl Mapo {
	fn new(lens: &Vec<usize>) -> Mapo {
		let flatLen = lens.iter().max();
		let mut flatVec [Option<HashMap<u32, LenValCnt>>; flatLen] = [None; flatLen];	
  	for x in lens {
			flatVec[x] = HashMap<u32, LenValCnt>::new();
		}
		return Mapo {
      lens: lens.clone(),
			lenMap : flatVec
		}
	}

	fn add(&mut self, len: usize, val: u32, cnt: u32) {
		self.lenMap[len].entry(val).or_insert(0) += cnt; 
	}

	fn for_each(&self, len: usize, ac: &Fn(usize, u32, u32)) {
		for i in self.lenMap[len].values() {
			ac(i.len, i.val, i.count);
		}
	} 
	fn reset() {
		for len in self.lens {
			self.lenMap[len].unwrap().clear();
		}
	}
}

fn main() {
    //infer_schema!("dotenv:DATABASE_URL");
    let start = 0;
    let step  = (4294967296 as usize)/ (8 as usize);
    let mut threads = Vec::new();
    let lens = vec![2,4,8];
    //let so: HashMap<usize, HashMap<u32, usize>> = HashMap::new();
    //let res_so = Arc::new(Mutex::new(so));
    create_mapos(&conn, lens);
    for yy in 0..8 {
	let y = yy.clone();
	//let tres_so = res_so.clone();
	threads.push(thread::spawn(move || {
    	    let conn = Connection::connect("postgresql://root:meno@localhost:5433/root", TlsMode::None).unwrap();
	    println!("Range for:{}:{:08x}-{:08x}", y, (step*y), (step*(y+1)));
	    //let mut my: HashMap<String, isize> = HashMap::new();
	    let mut my = Mapo::new(lens);
	    for x in (step*y)..(step*(y+1)) {
		let data = format!("{}", x);
		let result = hex_digest(Algorithm::SHA256, &transform_u32_to_array_of_u8(x as u32));
		//println!("{}", result)
		//split_by_bits(result, &mut|val, bits| {
		for val in split_by_bits(result) {
		 
		    //let mut so = tres_so.lock().unwrap();
		    my.add(val.len, val.val, 1);
		    /*
		    let x = match my.get(&val) {
			Some(s) => s+1,
			None => 1,
		    };
		    my.insert(val.clone(), x);
		    */
		}//);
		if (x % 20000) == 0 {
		    //println!("{}:{}:{}", y, x, tres_so.lock().unwrap().len());
		    println!("{}:{}", y, x);
		     
				let bb = BuildBatch::new();
				for len in lens {
		    	my.for_each(len, &mut|val, cnt| {
			    so.insert(val.clone(), x);
		    });
				}
				my.reset();
		}
	    }
	}));
      }
      while threads.len() > 0 {
	let first = threads.pop().unwrap();
	first.join();
      }
/*
*/
}
