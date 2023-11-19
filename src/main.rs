#![allow(dead_code, unused)]
use std::fs;
use std::fs::File;
use clap::Parser;
use serde::{Deserialize, Serialize};

mod block_organizer;
mod oracle_decoder;
mod buffer_organizer;
 
/// Tool for extracting raw data from datafiles
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    ///Size of memory segment to scan
    #[clap(short, long, default_value="params.json")]
    param_file: String,

    ///Manual recognition 
    #[clap(short, long, default_value="NO")]
    manual_string: String,

    /// Number of parallel threads
    #[clap(short, long, default_value_t=2, short='P')]
    parallel: u8,
}

#[derive(Serialize, Deserialize)]
struct Params {
    action: String,
    workdir: String,
    data_files: Vec<String>,
}

fn read_params(fname: &str) -> Params {
    let param_file = fs::read_to_string(fname).expect(&format!("Something wrong with a file {} ", fname));
    let v_params: Params = serde_json::from_str(&param_file).expect("Wrong JSON format");
    v_params
}

fn main() {
    let args = Args::parse(); 
    if args.manual_string == "NO" {
        let params = read_params(&args.param_file);
        
        if params.action == "consolidate objects" {
            for f in params.data_files {
                block_organizer::consolidate_objects_from_file(f, params.workdir.clone(), args.parallel);
            }
        } else if params.action == "extract data from file" {
            for f in params.data_files {
                block_organizer::extract_from_file(f, params.workdir.clone(), args.parallel);
            }
        } else if params.action == "consolidate objects from memory" {
            let pid: u32 = params.data_files[0].parse().unwrap();
            let memory_size: u64 = params.data_files[1].parse().unwrap();
            block_organizer::consolidate_objects_from_memory(pid, memory_size, params.workdir.clone(), args.parallel);
        } else if params.action == "visualize buffers" {
            let file_addresses = params.data_files[0].clone();
            let obj: u32 = params.data_files[1].parse().unwrap();
            let pid: u32 = params.data_files[2].parse().unwrap();
            buffer_organizer::visualize_buffers(file_addresses, obj, pid); 
        }
    }  else {
        println!("{:?}", oracle_decoder::guess_type_str(args.manual_string));
    }

}
 