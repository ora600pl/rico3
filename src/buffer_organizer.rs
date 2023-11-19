use std::default;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use binread::BinRead;
use binread::BinReaderExt;
use clap::Parser;
use std::{thread, time};
use std::fs::read_to_string;
use std::mem;


#[derive(BinRead)]
#[derive(Debug)]
#[allow(dead_code)]
struct Xbh {
    ckpt1: u64, //@0
    ckpt2: u64, //@8
    #[br(count = 160)]
    data1: Vec<u8>, //unknown data @16
    nxt_hash: u64, //@176
    prv_hash: u64, //@184
    #[br(count = 8)]
    data2: Vec<u8>, //unknown data @192
    rdba: u32,  //@200
    flag1: u32, //@204
    objd: u32,  //@208
    flag2: u32, //@212
    dirty_flag: u32, //@216
    #[br(count = 12)]
    data3: Vec<u8>, //unknown data @220
    nxt_repl: u64,   //@232
    prv_repl: u64,   //@240
    #[br(count = 3)]
    data4: Vec<u8>, //unknown data @248
    tch: u8, //@251
    #[br(count = 4)]
    data5: Vec<u8>, //unknown data @252
    ba: u64, //@256
    #[br(count = 96)]
    data6: Vec<u8>, //unknown data @264
}

fn read_bh_addr(fname: String) -> Vec<u64> {
    let mut addrs: Vec<u64> = Vec::new();
    for line in read_to_string(fname).unwrap().lines() {
        let addr = u64::from_str_radix(line, 16).unwrap();
        addrs.push(addr);
    }

    addrs
}

pub fn visualize_buffers(fname: String, objd: u32, pid: u32) {
    let mut dots = 0;
    let memfile = format!("/proc/{}/mem", pid);
    let mut f = File::open(memfile).unwrap();
    let mut addrs = read_bh_addr(fname);
    addrs.sort();
    let xbh_len = addrs.len();

    f.seek(SeekFrom::Start(addrs[0])).unwrap();
    
    let buffer_len = addrs[xbh_len-1] - addrs[0] + 360;
    let mut buffer = vec![0_u8; buffer_len as usize];
    f.read(&mut buffer); 

    let mut xbh_cursor = Cursor::new(buffer);

    let start_addr = addrs[0];
    for addr in addrs {
        let a = addr - start_addr; 
        xbh_cursor.seek(SeekFrom::Start(a)).unwrap();

        let xbh: Result<Xbh, binread::Error> = xbh_cursor.read_ne();
        if xbh.is_err() {
            break;
        }
        let xbh = xbh.unwrap();

        if xbh.objd == objd {
            print!("X");
        } else {
            dots+=1;
            if dots == 100 { 
                print!(".");
                dots = 0;
            }
        }
    } 
}