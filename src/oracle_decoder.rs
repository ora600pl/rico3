use hex;
use std::str;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use chrono::NaiveDateTime;

#[derive(Debug)]
pub struct OracleType {
    pub data_type: String,
    pub value: String,
}

fn check_string(bytes_val: Vec<u8>) -> Result<OracleType, String> {
    let utfstring = str::from_utf8(&bytes_val);

    if utfstring.is_ok() {
        let check_printable = utfstring.unwrap().to_string();
        if check_printable.chars().all(|x| x.is_alphanumeric() || x.is_ascii() || x.is_ascii_graphic()) {
            return Ok(OracleType{data_type: "VARCHAR2".to_string(), value: check_printable});
        } else {
            return Err("Not a string".to_string());
        }
    } else {
        return Err("Not a string".to_string());return Err("Not a string".to_string());
    }
}

fn check_date(bytes_val: Vec<u8>) -> Result<OracleType, String> {
    if bytes_val.len() != 7 {
        return Err("Not a date".to_string());
    }

    let century: i16 = bytes_val[0] as i16 - 100;
    if century <= 0 {
        return Err("Not a date".to_string());
    }

    let year: i16 = bytes_val[1] as i16 - 100;
    if year < 0 {
        return Err("Not a date".to_string());
    }

    let month: i8 = bytes_val[2] as i8;
    let day: i8 = bytes_val[3] as i8;
    let hour: i8 = bytes_val[4] as i8 - 1;
    let minute: i8 = bytes_val[5] as i8 -1;
    let second: i8 = bytes_val[6] as i8 - 1;

    if hour < 0 || minute < 0 || second < 0 {
        return Err("Not a date".to_string());
    }

    let date_string = format!("{:02}{:02}-{:02}-{:02} {:02}:{:02}:{:02}", century, year, month, day, hour, minute, second);
    let dt = NaiveDateTime::parse_from_str(&date_string, "%Y-%m-%d %H:%M:%S");
    if dt.is_err() {
        return Err("Not a date".to_string());
    }

    return Ok(OracleType{data_type: "DATE".to_string(), value: date_string});
    
}

fn check_timestamp(bytes_val: Vec<u8>) -> Result<OracleType, String> {
    if bytes_val.len() != 11 {
        return Err("Not a date".to_string());
    }

    let century: i16 = bytes_val[0] as i16 - 100;
    if century <= 0 {
        return Err("Not a date".to_string());
    }

    let year: i16 = bytes_val[1] as i16 - 100;
    if year < 0 {
        return Err("Not a date".to_string());
    }

    let month: i8 = bytes_val[2] as i8;
    let day: i8 = bytes_val[3] as i8;
    let hour: i8 = bytes_val[4] as i8 - 1;
    let minute: i8 = bytes_val[5] as i8 - 1;
    let second: i8 = bytes_val[6] as i8 - 1;
    let second_fraction: u32 = u32::from_be_bytes(bytes_val[7..11].try_into().unwrap());

    if hour < 0 || minute < 0 || second < 0 {
        return Err("Not a date".to_string());
    }

    let date_string = format!("{:02}{:02}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}", century, year, month, day, hour, minute, second, second_fraction);
    let dt = NaiveDateTime::parse_from_str(&date_string, "%Y-%m-%d %H:%M:%S%.f");
    if dt.is_err() {
        return Err("Not a date".to_string());
    }

    return Ok(OracleType{data_type: "TIMESTAMP".to_string(), value: date_string});
    
}


fn check_number(bytes_val: Vec<u8>) -> Result<OracleType, String> {
    if bytes_val.len() == 1 && bytes_val[0] == 128 {
        return Ok(OracleType { data_type: "NUMBER".to_string(), value: "0".to_string() });
    }

    let mut exp = 0;
    let mut number_value = "0.".to_string();

    let last_byte_idx = bytes_val.len() - 1;
    if (bytes_val[last_byte_idx] != 102 && bytes_val[0] < 193) || bytes_val[0] > 208 {
        return Err("Not a NUMBER".to_string());
    } else if bytes_val[last_byte_idx] != 102 && bytes_val[0] >= 193 {
        exp = (bytes_val[0] - 193) * 2 + 2;
        
        for i in 1..last_byte_idx+1 {
            number_value = format!("{}{:02}", number_value, bytes_val[i] - 1);
        }
        
    } else if bytes_val[last_byte_idx] == 102 && bytes_val[0] <= 62 {
        number_value = "-0.".to_string();
        exp = (62 - bytes_val[0]) * 2 + 2;

        for i in 1..last_byte_idx+1 {
            if (101 as i64 - bytes_val[i] as i64) < 0 {
                return Err("Not a NUMBER".to_string());
            }
            number_value = format!("{}{:02}", number_value, 101 - bytes_val[i]);
        }

    } else {
        return Err("Not a NUMBER".to_string());
    }

    let number_decimal = Decimal::from_str(&number_value);
    if number_decimal.is_err() {
        return Err("Not a NUMBER".to_string());
    }

    let mut number_decimal = number_decimal.unwrap();
    for i in 0..exp {
        let check_overflow =  number_decimal.checked_mul(dec!(10));
        if check_overflow.is_none() {
            return Err("Not a NUMBER".to_string());
        }
        number_decimal = number_decimal * dec!(10);
    }

    return Ok(OracleType { data_type: "NUMBER".to_string(), value: number_decimal.to_string() });

}

pub fn guess_type(byte_intput: Vec<u8>) -> OracleType {
    //println!("\t\tGuessing {:x?}", byte_intput.as_slice());
    if byte_intput.len() == 0 {
        return OracleType{data_type: "Unrecognized".to_string(), value: "NONE".to_string()};
    }
    if byte_intput[0] == 255 {
        return OracleType{data_type: "NULL".to_string(), value: "NULL".to_string()};
    }

    let result = check_date(byte_intput.clone());
    if result.is_ok() {
        return result.unwrap();
    }

    let result = check_timestamp(byte_intput.clone());
    if result.is_ok() {
        return result.unwrap();
    }
    
    let result = check_string(byte_intput.clone());
    if result.is_ok() {
        return result.unwrap();
    } 
    
    let result = check_number(byte_intput.clone());
    if result.is_ok() {
        return result.unwrap();
    } 

    return OracleType{data_type: "Unrecognized".to_string(), value: "NONE".to_string()};
}

pub fn guess_type_str(string_val: String) -> OracleType {
    let byte_intput = hex::decode(string_val).unwrap();
    return guess_type(byte_intput.clone());
}

