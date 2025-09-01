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

use cc_host_mapper::*;
use clap::Parser;
use dialoguer::{Confirm, Input};

#[derive(Parser)]
struct Opts {
    /// Output file name
    #[arg(short, long)]
    output: Option<String>,

    /// Number of threads to be used for crawling
    #[arg(short, long)]
    threads: Option<usize>,

    /// Index wanted to crawl from
    #[arg(short, long)]
    index_id: Option<String>,

    /// Dump cluster index to CSV file
    #[arg(short, long)]
    dump_cluster_idx: bool,
}

fn main() {
    let opts: Opts = Opts::parse();
    let mut index_list: Vec<Index> = retrieve_indices();
    // Sort the list first to get newest-first order
    index_list.sort();

    let ids = &index_list
        .iter()
        .cloned()
        .map(|x| x.id)
        .collect::<Vec<String>>();
    let ids_str = ids.join(",");

    let mut selected_index;

    match opts.index_id {
        Some(index_id) => {
            selected_index = match index_list.iter().find(|x| x.id == index_id) {
                Some(index) => index.clone(),
                None => panic!("index id {} not found", index_id),
            }
        }

        None => {
            selected_index = index_list[0].to_owned();

            if !Confirm::new()
                .with_prompt(format!("Do you want to crawl index {}?", selected_index.id))
                .default(false)
                .interact()
                .unwrap()
            {
                // we don't want to go with the most recent
                if !Confirm::new()
                    .with_prompt("Do you want to crawl another index?".to_string())
                    .default(false)
                    .interact()
                    .unwrap()
                {
                    // we don't want to select one
                    println!("nevermind then :)");
                    return;
                } else {
                    // select one index from list
                    let input: String = Input::new()
                        .with_prompt(format!(
                            "Select from the following index IDs:\n{}",
                            ids_str.as_str()
                        ))
                        .interact_text()
                        .unwrap();

                    match index_list.iter().find(|x| x.id == input) {
                        Some(index) => selected_index = index.clone(),
                        None => return,
                    }
                }
            }
        }
    }

    let output_file_name = match opts.output {
        Some(output) => output,
        None => {
            format!(
                "mapping-{}.csv.gz",
                selected_index.id.as_str().to_lowercase()
            )
        }
    };

    if opts.dump_cluster_idx {
        println!("dumping cluster.idx to csv file");
        let host_pointers = read_cluster_idx(&selected_index.id.to_owned());
        let mut writer = get_writer(&format!(
            "cluster-idx-{}.csv.gz",
            selected_index.id.as_str().to_lowercase()
        ));
        for item in host_pointers {
            writeln!(writer, "{}", item.to_csv()).unwrap();
        }
        return;
    }

    println!("Will start crawling {} now...", selected_index.id);
    crawl_host_ip_mapping(
        selected_index.id.to_owned(),
        output_file_name.to_owned(),
        opts.threads,
    );
}
