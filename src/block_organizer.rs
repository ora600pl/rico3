use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use binread::BinRead;
use binread::BinReaderExt;
use binread::until_exclusive;
use std::io::Cursor;
use std::io::Read;
use std::collections::HashMap;
use oracle::Connection;
use rand::Rng;
use std::mem;
use std::thread;
use crossbeam_channel::bounded;
use crossbeam_channel::Receiver;
use crate::oracle_decoder::OracleType;
use crate::oracle_decoder;
use proc_maps::{get_process_maps, Pid};
use std::time::SystemTime;
use chrono::DateTime;
use chrono::Utc;

const BLOCK_SIZE: u64 = 8192;


#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Kcbh {
    type_kcbh: u8,
    frmt_kcbh: u8,
    spare1_kcbh: u8,
    spare2_kcbh: u8,
    rdba_kcbh: u32,
    bas_kcbh: u32,
    wrp_kcbh: u16,
    seq_kcbh: u8,
    flg_kcbh: u8,
    chkval_kcbh: u16,
    spare3_kcbh: u16
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Ktbbhitl {
    kxidusn: u16,
    kxidslt: u16,
    kxidsqn: u32,
    kubadba: u32,
    kubaseq: u16,
    kubarec: u8,
    _spare1: u8,
    ktbitflg: u16,
    ktbitun: u16,
    ktbitbas: u32,
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Ktbbhcsc
{
    kscnbas: u32,
    kscnwrp: u32,
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Ktbbh {
    ktbbhtyp: u32,
    ktbbhsid: u32,
    ktbbhcsc: Ktbbhcsc,
    ktbbhict: u8,
    _something: u8,
    ktbbhflg: u8,
    ktbbhfsl: u8,
    ktbbhfnx: u32,
    #[br(count = ktbbhict)] 
    ktbbhitl: Vec<Ktbbhitl>,
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Kdbh {
    kdbhflag: u8,
    kdbhntab: i8,
    kdbhnrow: i16,
    kdbhfrre: i16,
    kdbhfseo: i16,
    kdbhfsbo: i16,
    kdbhavsp: i16,
    kdbhtosp: i16,
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct Kdbt {
    kdbtoffs: i16,
    kdbtnrow: i16,
}

#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
#[derive(Clone)]
struct OracleBlockTable {
    kcbh: Kcbh,
    ktbbh: Ktbbh,
    kdbh: Kdbh,
    kdbt: Vec<Kdbt>,    
    //kdbr: Vec<i16>,
}

#[derive(BinRead)]
#[derive(Debug)]
struct ModFlags {
    flag1: u32,
    flag2: u32,
}

#[derive(BinRead)]
struct OracleBlock {
    #[br(count = BLOCK_SIZE)]
    block_data: Vec<u8>,
}

#[derive(BinRead)]
struct ColumnData {
    col_len: u8,
    #[br(count=col_len)]
    col_data: Vec<u8>,
}

#[derive(BinRead)]
struct ColumnDataLong {
    col_len: u16,
    #[br(count=col_len)]
    col_data: Vec<u8>,
}
 
fn write_bytes_to_file(fname: String, bytes_val:Vec<u8>) {
    let mut f_obj = File::options().append(true).create(true).open(fname).unwrap();
    f_obj.write_all(&bytes_val);
    f_obj.flush();
}

fn write_text_to_file(fname: String, line: String) {
    let mut f_obj = File::options().append(true).create(true).open(fname).unwrap();
    let line_new = format!("{}\n", line);
    f_obj.write_all(line_new.as_bytes());
}

fn write_log(workdir: String, action: String) {
    let now = SystemTime::now();
    let dt: DateTime<Utc> = now.into();
    let fname = format!("{}/rico3.log",workdir);

    let logline = format!("{}\t{}", dt.format("%Y%m%d%H%M%S"), action);
    let mut f_obj = File::options().append(true).create(true).open(fname).unwrap();

    f_obj.write_all(logline.as_bytes());
    f_obj.flush();
}

fn consolidate_chunk(chunk_bytes: Vec<u8>, workdir: String) {
    let chunk_len = chunk_bytes.len();
    let chunk_len_blocks = chunk_len as u64 / BLOCK_SIZE;
    let mut position = 0;
    let scan_to = chunk_len_blocks;
    while position < scan_to {
        let block_data = &chunk_bytes[(position*BLOCK_SIZE) as usize..(position+1) as usize * BLOCK_SIZE as usize];
        if block_data[0] == 6 && block_data[20] == 1 {
            let objd = u32::from_ne_bytes(block_data[24..28].try_into().unwrap());
            let f_obj_name = format!("{}/{}.dat", workdir, objd);
            write_bytes_to_file(f_obj_name, block_data.to_vec());
        }
        position += 1;
    }
}

fn consolidate_chunk_parallel(rc: Receiver<Vec<u8>>, workdir: String, worker_id: u8) {
    println!("Starting worker {}", worker_id);
    for chunk_bytes in rc {
        
        let chunk_len = chunk_bytes.len();
        let chunk_len_blocks = chunk_len as u64 / BLOCK_SIZE;
        let mut position = 0;
        let scan_to = chunk_len_blocks;
        while position < scan_to {
            let block_data = &chunk_bytes[(position*BLOCK_SIZE) as usize..(position+1) as usize * BLOCK_SIZE as usize];
            if block_data[0] == 6 && block_data[20] == 1 {
                let objd = u32::from_ne_bytes(block_data[24..28].try_into().unwrap());
                let f_obj_name = format!("{}/{}.dat", workdir, objd);
                write_bytes_to_file(f_obj_name, block_data.to_vec());
            }
            position += 1;
        }
    }
    println!("Stopping worker {}", worker_id);
}

pub fn consolidate_objects_from_memory(pid: u32, memory_size: u64, workdir: String, parallel: u8) {
    println!("Processing pid {} for memory size {}", pid, memory_size);
    let maps = get_process_maps(pid as Pid).unwrap();
    let mut scan_from: u64 = 0;
    let mut scan_to: u64 = 0;
    for map in maps {
        if (map.size() as u64) == memory_size {
            scan_from = map.start() as u64;
            scan_to   = scan_from + (map.size() as u64);
            println!("Found map at the start offset = {} \t end offset = {}\n", scan_from, scan_to);
            break;
        }
    }

    

}

pub fn consolidate_objects_from_file(fname: String, workdir: String, parallel: u8) {
    println!("Processing file {} ", fname);
    let mut buffer = [0; 1_048_576];
    let mut f = File::open(&fname).unwrap(); 

    let (tx, rx) = bounded::<Vec<u8>>(parallel as usize);
    let mut threads: Vec<thread::JoinHandle<_>> = Vec::new();
    for p in 0..parallel  {
        let rx = rx.clone();
        let w = workdir.clone();
        threads.push(thread::spawn(move || {consolidate_chunk_parallel(rx, w, p)}));
    }

    loop {
        let res = f.read(&mut buffer);
        if res.is_err() {
            break;
        }
        let out_bytes = res.unwrap();
        if out_bytes == 0 {
            break;
        }
        
        tx.send(buffer.to_vec());
    }
    drop(tx);
    for t in threads {
        t.join().unwrap();
    }
}

pub fn extract_from_file(fname: String, workdir: String, parallel: u8) {
    println!("Processing file {} ", fname);
    let mut buffer = [0; 1_048_576];
    let file_path = format!("{}/{}", workdir, fname);
    let mut f = File::open(&file_path).unwrap(); 

    let (tx, rx) = bounded::<Vec<u8>>(parallel as usize);
    let mut threads: Vec<thread::JoinHandle<_>> = Vec::new();
    for p in 0..parallel  {
        let rx = rx.clone();
        let w = workdir.clone();
        threads.push(thread::spawn(move || {extract_chunk_parallel(rx, w, p)}));
    }

    loop {
        let res = f.read(&mut buffer);
        if res.is_err() {
            break;
        }
        let out_bytes = res.unwrap();
        if out_bytes == 0 {
            break;
        }

        tx.send(buffer.to_vec());
    }
    drop(tx);
    for t in threads {
        t.join().unwrap();
    }
}

fn extract_chunk_parallel(rc: Receiver<Vec<u8>>, workdir: String, worker_id: u8) {
    println!("Starting worker {}", worker_id);
    for chunk_bytes in rc {
        
        let chunk_len = chunk_bytes.len();
        let chunk_len_blocks = chunk_len as u64 / BLOCK_SIZE;
        let mut position = 0;
        let scan_to = chunk_len_blocks;
        while position < scan_to {
            let block_data = &chunk_bytes[(position*BLOCK_SIZE) as usize..(position+1) as usize * BLOCK_SIZE as usize];
            if block_data[0] == 6 && block_data[20] == 1 {
                write_log(workdir.clone(), format!("Trying to extract from block {}", position));
                extract_block(block_data.to_vec(), workdir.clone());
            }
            position += 1;
        }
    }
    println!("Stopping worker {}", worker_id);
}

fn extract_block(block_data: Vec<u8>, workdir: String) {
    let mut block_cursor = Cursor::new(block_data);
    let kcbh: Kcbh = block_cursor.read_ne().unwrap();
    let ktbbh: Ktbbh = block_cursor.read_ne().unwrap();

    let end_of_ktbbh = block_cursor.seek(SeekFrom::Current(0)).unwrap() as i64;

    let mod_flags: ModFlags = block_cursor.read_ne().unwrap();
    let mut offset_mod: i64 = 0;
    if mod_flags.flag1 == 0 && mod_flags.flag2 == 0 {
        offset_mod = 8;
    } 

    let kdbh_offset = end_of_ktbbh + offset_mod;
    block_cursor.seek(SeekFrom::Start(kdbh_offset as u64)).unwrap();

    let kdbh: Kdbh = block_cursor.read_ne().unwrap();

    let mut kdbt: Vec<Kdbt> = Vec::new();
    for i in 0..kdbh.kdbhntab {
        let k: Kdbt = block_cursor.read_ne().unwrap();
        kdbt.push(k);
    }

    let ob: OracleBlockTable = OracleBlockTable { kcbh: kcbh.clone(), ktbbh: ktbbh.clone(), kdbh: kdbh.clone(), kdbt: kdbt.clone() };
    write_log(workdir.clone(), format!("{:#?}", ob));

    let delcared_rows_offset = end_of_ktbbh + offset_mod + 2;

    let mut row_string: String = String::new();

    let mut row_pointer_mod: i64 = 0;
    if mod_flags.flag1 == 0 && mod_flags.flag2 == 0 {
        row_pointer_mod = 100;
    } else if mod_flags.flag1 == 0 && mod_flags.flag2 > 0 {
        row_pointer_mod = 96;
    } else if mod_flags.flag1 > 0 && mod_flags.flag2 > 0 {
        row_pointer_mod = 92; 
    }

    if kdbh.kdbhnrow > 0 {
        let num_of_tables_offset = delcared_rows_offset - 1;
        block_cursor.seek(SeekFrom::Start(num_of_tables_offset as u64)).unwrap();
        let num_of_tables: i8 = block_cursor.read_ne().unwrap();
        let mut row_pointer_offset = end_of_ktbbh + offset_mod + 14 + 4 * num_of_tables as i64;

        let mut deleted_rows = 0;

        for i in 0..kdbh.kdbhnrow {
            
            
            block_cursor.seek(SeekFrom::Start(row_pointer_offset as u64)).unwrap();
            let row_pointer: i16 = block_cursor.read_ne().unwrap();
            let mut row_pointer: i64 = row_pointer as i64;
            row_pointer += row_pointer_mod + 24 * ((ktbbh.ktbbhict as i64) - 2);

            if row_pointer as i64 > (2 * kdbh.kdbhnrow as i64 + end_of_ktbbh + offset_mod + 14 + 4 * num_of_tables as i64) && row_pointer as u64 <= BLOCK_SIZE - 8 {

                block_cursor.seek(SeekFrom::Start(row_pointer as u64)).unwrap();
                let row_header: u8 = block_cursor.read_ne().unwrap();
                write_log(workdir.clone(), format!("\tProcessing row {} at pointer {}", i, row_pointer));

                if row_header == 44 {
                    block_cursor.seek(SeekFrom::Start(row_pointer as u64 + 2)).unwrap();
                    let no_columns: u8 = block_cursor.read_ne().unwrap();
                    block_cursor.seek(SeekFrom::Start(row_pointer as u64 + 3)).unwrap();
                    for i in 0..no_columns {
                        let mut column_data_value: OracleType =  OracleType { data_type: "NULL".to_string(), value: "NULL".to_string() };
                        let col_len: u8 = block_cursor.read_ne().unwrap();

                        if col_len == 254 {
                            let column_data: ColumnDataLong = block_cursor.read_ne().unwrap();
                            column_data_value = oracle_decoder::guess_type(column_data.col_data);
                        } else if col_len < 254 {
                            block_cursor.seek(SeekFrom::Current(-1));
                            let column_data: ColumnData = block_cursor.read_ne().unwrap();
                            column_data_value = oracle_decoder::guess_type(column_data.col_data);
                        }

                        row_string = format!("{}|{}", row_string, column_data_value.value);
                        
                    }
                } else if row_header == 60 {
                    deleted_rows += 1;
                }

            }

            row_pointer_offset += 2;
            if row_string.len() > 1 {
                write_text_to_file(format!("{}/{}.csv", workdir.clone(), ktbbh.ktbbhsid), row_string.clone());
            }
            row_string.clear();
        }
    }

}