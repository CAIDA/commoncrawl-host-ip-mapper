/*
 * This software is Copyright (c) 2021 The Regents of the University of
 * California. All Rights Reserved. Permission to copy, modify, and distribute this
 * software and its documentation for academic research and education purposes,
 * without fee, and without a written agreement is hereby granted, provided that
 * the above copyright notice, this paragraph and the following three paragraphs
 * appear in all copies. Permission to make use of this software for other than
 * academic research and education purposes may be obtained by contacting:
 *
 * Office of Innovation and Commercialization
 * 9500 Gilman Drive, Mail Code 0910
 * University of California
 * La Jolla, CA 92093-0910
 * (858) 534-5815
 * invent@ucsd.edu
 *
 * This software program and documentation are copyrighted by The Regents of the
 * University of California. The software program and documentation are supplied
 * "as is", without any accompanying services from The Regents. The Regents does
 * not warrant that the operation of the program will be uninterrupted or
 * error-free. The end-user understands that the program was developed for research
 * purposes and is advised not to rely exclusively on the program for any reason.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
 * PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
 * THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

//! CommonCrawl Host Mapper crawls the select CommonCrawl index and generate
//! host to IP mapping file.
//!
//! It is designed to be massively parallel. Depending on the capacity of the
//! runtime system, user can run the crawling on tens or hundreds of threads to
//! speed up the retrival process.
//!
//! It also comes with very straightforward commandline user interface and
//! progress bar on the current crawling process.
use chrono::prelude::*;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use reqwest::{
    self,
    header::{HeaderValue, RANGE},
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::sync::mpsc::channel;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader, BufWriter, Write},
    net::IpAddr,
    thread,
};

const BASE_URL: &str = "https://data.commoncrawl.org";

/// An index is a set of [IndexFiles] that logs the locations of the WARC
/// records for the hosts Common Crawl crawled for that period
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Index {
    /// Index ID, e.g. `CC-MAIN-2020-50`
    pub id: String,
    /// Name, e.g. `November 2020 Index`
    pub name: String,
    /// e.g. `https://index.commoncrawl.org/CC-MAIN-2020-50/`
    pub timegate: String,
    /// e.g. `https://index.commoncrawl.org/CC-MAIN-2020-50-index`
    #[serde(rename(deserialize = "cdx-api"))]
    pub cdx_api: String,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Index {}

impl Ord for Index {
    fn cmp(&self, other: &Self) -> Ordering {
        // Try to parse as dates first
        let d1 = NaiveDate::parse_from_str(&self.name, "%B %Y Index");
        let d2 = NaiveDate::parse_from_str(&other.name, "%B %Y Index");

        match (d1, d2) {
            (Ok(date1), Ok(date2)) => date1.cmp(&date2),
            // If either fails to parse, fall back to string comparison
            _ => self.name.cmp(&other.name),
        }
    }
}

impl PartialOrd for Index {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Each [Index] contains a list of actual index files
#[derive(Debug)]
pub struct IndexFiles {
    pub cdx_files: Vec<String>,
    pub cdx_cluster: String,
    pub metadata: String,
}

/// A line in cluster.idx file that points to a record on one index file for the
/// host in question.
#[derive(Debug, Clone)]
pub struct IndexHostPointer {
    pub host: String,
    pub timestamp: i64,
    pub index_file_name: String,
    pub range_start: i64,
    pub range_length: i64,
}

impl IndexHostPointer {
    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.host, self.timestamp, self.index_file_name, self.range_start, self.range_length
        )
    }
}

/// A record in an index file.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct IndexRecord {
    url: String,
    mime: String,
    #[serde(rename(deserialize = "mime-detected"))]
    mime_detected: Option<String>,
    status: String,
    digest: Option<String>,
    length: String,
    offset: String,
    filename: String,
}

/// Host to IP mapping entry. This is the final product.
#[derive(Debug, Clone)]
pub struct MappingEntry {
    pub host: String,
    pub timestr: String,
    pub ip: IpAddr,
}

#[allow(dead_code)]
fn parse_index(index_id: &str) -> IndexFiles {
    let path_file = format!("{}/crawl-data/{}/cc-index.paths.gz", BASE_URL, index_id);

    let bytes: Vec<u8> = reqwest::blocking::get(&path_file)
        .unwrap()
        .bytes()
        .unwrap()
        .to_vec();

    // NOTE: needs both of the following imports BufRead, BufReader;
    let reader = BufReader::new(GzDecoder::new(&*bytes));

    let mut idx = IndexFiles {
        cdx_files: vec![],
        cdx_cluster: "".to_string(),
        metadata: "".to_string(),
    };

    for line in reader.lines() {
        let temp_line = line.unwrap();
        let line_string = format!("{}/{}", BASE_URL, temp_line);
        if let Some(name) = temp_line.split("/").last() {
            match name {
                "cluster.idx" => idx.cdx_cluster = line_string,
                "metadata.yaml" => idx.metadata = line_string,
                _ => idx.cdx_files.push(line_string),
            }
        }
    }

    idx
}

/// Retrieve a number of indices using commoncrawl's
/// [`colinfo`](https://index.commoncrawl.org/collinfo.json) json API.
///
/// There is no guarantee the indices will be sorted. If sorted indices are
/// desired, run `.sort()` function on the returned Vec. The default sorting
/// order is most-recent-first.
///
/// # Example
///
/// Retrieve all indices and sort by most-recent-first order.
/// ```no_run
/// let mut index_list: Vec<Index> = retrieve_indices();
/// index_list.sort();
/// // index_list.reverse();
/// ```
pub fn retrieve_indices() -> Vec<Index> {
    let rsp = match reqwest::blocking::get("https://index.commoncrawl.org/collinfo.json") {
        Ok(a) => a,
        _ => unreachable!(),
    };
    match rsp.json::<Vec<Index>>() {
        Ok(lst) => lst,
        Err(_) => panic!("cannot parse returned json to struct"),
    }
}

/// Retrives all indicis using [retrieve_indices] functions, sorts the indicis
/// using the names, and return the most recent index.
///
/// The sorting is done by parsing Index names (e.g. `November 2020 Index`) to
/// [NaiveDate] and compare the dates.
pub fn get_newest_index() -> Index {
    let mut indices = retrieve_indices();
    indices.sort();
    indices[0].clone()
}

/// Parse one line in cluster.idx file and return a [IndexHostPointer]
///
/// Example line:
/// 0,102,126,13:7037)/robots.txt 20201126201142\tcdx-00000.gz\t0\t205505\t1
fn parse_idx_entry(index_id: &str, line: String) -> Option<IndexHostPointer> {
    let parts: Vec<&str> = line.split("\t").collect::<Vec<&str>>();
    assert_eq!(parts.len(), 5);
    let url_time = parts[0].split(" ").collect::<Vec<&str>>();
    let timestamp = url_time[1].parse::<i64>().unwrap();
    let mut host_vec = url_time[0].split(")").collect::<Vec<&str>>()[0]
        .split(",")
        .collect::<Vec<&str>>();
    if host_vec[0].chars().all(char::is_numeric) {
        // it is a IP address, not a host name
        return None;
    }

    host_vec.reverse();
    let host = host_vec.join(".");

    let file_name = format!(
        "{}/cc-index/collections/{}/indexes/{}",
        BASE_URL,
        index_id,
        parts[1].parse::<String>().unwrap()
    );

    Some(IndexHostPointer {
        host,
        timestamp,
        index_file_name: file_name,
        range_start: parts[2].parse::<i64>().unwrap(),
        range_length: parts[3].parse::<i64>().unwrap(),
    })
}

/// Read the cluster.idx file to get a vector of HostPointers each of each
/// points to a location on a index file which in turn points to a location of a
/// WARC file.
///
/// Example line in an cluster.idx file:
/// `0,102,126,13:7037)/robots.txt 20201126201142\tcdx-00000.gz\t0\t205505\t1`
///
/// Essentially, these pointers will lead us to the location of the WARC records
/// for each host
pub fn read_cluster_idx(index_id: &str) -> Vec<IndexHostPointer> {
    let url = format!(
        "{}/cc-index/collections/{}/indexes/cluster.idx",
        BASE_URL, index_id
    );
    let stream = reqwest::blocking::get(url.to_owned())
        .unwrap()
        .bytes()
        .unwrap()
        .to_vec();
    let reader = BufReader::new(&*stream);

    let mut pointers = vec![];

    for line in reader.lines() {
        if let Some(host_pointer) = parse_idx_entry(index_id, line.unwrap()) {
            pointers.push(host_pointer);
        }
    }
    pointers
}

/// Query host IP using a [IndexHostPointer]. The pointer points to a location
/// on one index file for the host. This function will crawl the partial index
/// file to get the pointer to a WARC record and then crawl the WARC record to
/// get the actual IP.
pub fn query_host(pointer: IndexHostPointer) -> Vec<Option<MappingEntry>> {
    // TODO: should return Err and retry.
    let url = &pointer.index_file_name;
    let start = &pointer.range_start;
    let end = start + pointer.range_length;
    let client = reqwest::blocking::Client::new();

    let range_str = format!("bytes={}-{}", start, end);
    let range = HeaderValue::from_str(&range_str).unwrap();
    let rsp = match client.get(url).header(RANGE, range).send() {
        Ok(res) => res,
        Err(_) => return vec![],
    };

    // Check HTTP status before assuming gzipped content
    if !rsp.status().is_success() {
        eprintln!("HTTP error {}: {}", rsp.status(), url);
        return vec![];
    }

    let bytes = match rsp.bytes() {
        Ok(b) => b,
        Err(_) => return vec![],
    };
    drop(client);

    // NOTE: needs both of the following imports BufRead, BufReader;
    let reader = BufReader::new(GzDecoder::new(&*bytes));
    let mut records = vec![];
    let mut futures_times = HashSet::new();
    let mut mappings = vec![];

    for line in reader.lines() {
        let record_str = line.unwrap();
        let fields = record_str.split(" ").collect::<Vec<&str>>();

        // get host
        let mut host_vec = fields[0].split(")").collect::<Vec<&str>>()[0]
            .split(":")
            .collect::<Vec<&str>>()[0]
            .split(",")
            .collect::<Vec<&str>>();
        host_vec.reverse();
        let host = host_vec.join(".");

        // it's possible that the range provided contains records for other hosts, in this case, ignore
        if pointer.host != host {
            continue;
        }

        // get timestamp
        let date = parse_time_string(fields[1]);
        let timestamp_str = date.format("%Y-%m-%d").to_string();

        if !futures_times.contains(&timestamp_str) {
            let json_str = fields[2..].join(" ");
            if let Ok(entry) = serde_json::from_str::<IndexRecord>(json_str.as_str()) {
                mappings.push(retrieve_ip(
                    host.clone(),
                    timestamp_str.clone(),
                    entry.clone(),
                ));
                futures_times.insert(timestamp_str);
                records.push(entry);
            };
        }
    }

    mappings
}

fn parse_time_string(time_str: &str) -> chrono::DateTime<chrono::Utc> {
    let year = time_str[0..=3].parse::<i32>().unwrap();
    let month = time_str[4..=5].parse::<u32>().unwrap();
    let day = time_str[6..=7].parse::<u32>().unwrap();
    match NaiveDate::from_ymd_opt(year, month, day) {
        Some(date) => Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap()),
        None => {
            eprintln!("Invalid date: {}-{:02}-{:02}", year, month, day);
            Utc::now()
        }
    }
}

/// retrieve IP address of a crawl result from the WARC file specified in the index record
fn retrieve_ip(
    host: String,
    timestamp_str: String,
    index_record: IndexRecord,
) -> Option<MappingEntry> {
    let url = format!("{}/{}", BASE_URL, index_record.filename);
    let start: i64 = index_record.offset.parse::<i64>().unwrap();
    let mut length: i64 = index_record.length.parse::<i64>().unwrap();
    if length > 901 {
        length = 901;
    }
    let end: i64 = start + length;

    let range_str = format!("bytes={}-{}", start, end);
    let range = HeaderValue::from_str(&range_str).unwrap();
    let client = reqwest::blocking::Client::new();
    let rsp = match client.get(&url).header(RANGE, range).send() {
        Ok(res) => res,
        Err(_) => return None,
    };

    // Check HTTP status before assuming gzipped content
    if !rsp.status().is_success() {
        eprintln!("HTTP error {}: {}", rsp.status(), &url);
        return None;
    }

    let bytes = match rsp.bytes() {
        Ok(b) => b,
        Err(_) => return None,
    };
    let reader = BufReader::new(GzDecoder::new(&*bytes));
    // let reader = BufReader::new(&*bytes);
    for line in reader.lines() {
        match line {
            Ok(line) => {
                if line.starts_with("WARC-IP-Address") {
                    if let Ok(addr) = line.split(": ").collect::<Vec<&str>>()[1].parse::<IpAddr>() {
                        drop(client);
                        return Some(MappingEntry {
                            host: host.to_owned(),
                            timestr: timestamp_str,
                            ip: addr,
                        });
                    }
                }
            }
            Err(_) => break,
        }
    }
    drop(client);
    None
}

pub fn get_writer(filename: &str) -> Box<dyn Write> {
    let path = Path::new(filename);
    let file = match File::create(path) {
        Err(why) => panic!("couldn't open {}: {}", path.display(), why),
        Ok(file) => file,
    };
    if path.extension() == Some(OsStr::new("gz")) {
        // Error is here: Created file isn't gzip-compressed
        Box::new(BufWriter::with_capacity(
            128 * 1024,
            GzEncoder::new(file, Compression::default()),
        ))
    } else {
        Box::new(BufWriter::with_capacity(128 * 1024, file))
    }
}

/// All-in-one entry-point for multi-threaded crawling of host-to-IP mapping for one given CommonCrawl index.
///
/// # Examples
///
/// Get the newest index using [get_newest_index] function, and run crawling
/// with default number of threads (CPUs in the current system), and output
/// results to `mapping.csv`.
///
/// ```no_run
/// let newest_index = get_newest_index();
/// crawl_host_ip_mapping(newest_index.id.to_owned(), "mapping.csv".to_owned(), None);
/// ```
///
/// You can also specify the number of threads you want. For example, run crawling with 16 threads:
///
/// ```no_run
/// let newest_index = get_newest_index();
/// crawl_host_ip_mapping(newest_index.id.to_owned(), "mapping.csv".to_owned(), Some(16));
/// ```
pub fn crawl_host_ip_mapping(
    index_id: String,
    output_file_name: String,
    num_threads: Option<usize>,
) {
    let host_pointers = read_cluster_idx(&index_id);
    let total_hosts = host_pointers.len() as u64;

    let (sender, receiver) = channel::<MappingEntry>();
    let (sender_pb, receiver_pb) = channel::<String>();

    // dedicated thread for handling output of results
    let writer_thread = thread::spawn(move || {
        let mut writer = get_writer(output_file_name.as_str());
        for item in receiver.iter() {
            writeln!(writer, "{},{},{}", item.host, item.timestr, item.ip).unwrap();
        }
    });

    // dedicated thread for showing progress of the parsing
    thread::spawn(move || {
        let sty = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .expect("Invalid progress bar template")
            .progress_chars("##-");
        let pb = ProgressBar::new(total_hosts);
        pb.set_style(sty);
        for host in receiver_pb.iter() {
            pb.set_message(host.clone());
            pb.inc(1);
        }
    });

    // update number of threads to use if specified
    if let Some(num_t) = num_threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_t)
            .build_global()
            .expect("Failed to initialize rayon threadpool.");
    }

    println!("Will run in {} threads", rayon::current_num_threads());

    // start the actual crawling
    host_pointers
        .par_iter()
        .for_each_with((sender, sender_pb), |(s1, s2), x| {
            for mapping in query_host(x.clone()).into_iter().flatten() {
                s1.send(mapping.clone()).unwrap()
            }
            s2.send(x.host.to_owned()).unwrap();
        });

    // wait for the output thread to stop
    writer_thread.join().unwrap();
}
