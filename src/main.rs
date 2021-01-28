use cc_host_mapper::*;
use clap::Clap;
use dialoguer::{
    Confirm,
    Input,
};

#[derive(Clap)]
struct Opts {
    /// Output file name
    #[clap(short, long)]
    output: Option<String>,

    /// Number of threads to be used for crawling
    #[clap(short, long)]
    threads: Option<usize>,
}

fn main() {
    let opts: Opts = Opts::parse();

    let mut index_list: Vec<Index> = retrieve_indices();
    index_list.sort();
    let mut selected_index = index_list[0].to_owned();

    if !Confirm::new()
        .with_prompt(format!("Do you want to crawl index {}?", selected_index.id))
        .default(false)
        .interact()
        .unwrap()
    {
        // we don't want to go with the most recent
        if !Confirm::new()
            .with_prompt(format!("Do you want to crawl another index?"))
            .default(false)
            .interact()
            .unwrap(){
                // we don't want to select one
                println!("nevermind then :)");
                return;
            } else {
                // select one index from list
                let ids = &index_list.iter().cloned().map(|x| x.id).collect::<Vec<String>>();
                let ids_str = ids.join(",");

                let input: String = Input::new()
                    .with_prompt(format!("Select from the following index IDs:\n{}", ids_str.as_str()))
                    .interact_text().unwrap();

                match ids.iter().position(|x| x == &input) {
                    Some(index) => selected_index = index_list[index].clone(),
                    None => return
                }
            }
    }

    let output_file_name = match opts.output {
        Some(output) => output,
        None => {
            format!("mapping-{}.csv.gz", selected_index.id.as_str().to_lowercase())
        }
    };
    println!("Will start crawling {} now...", selected_index.id);
    crawl_host_ip_mapping(
        selected_index.id.to_owned(),
        output_file_name.to_owned(),
        opts.threads,
    );
}
