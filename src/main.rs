use std::{env, thread::available_parallelism};

use crate::processed_sites::{get_json_paths, process_sites};

mod line;
mod processed_sites;
mod site;
mod tag;

const DATA_PATH: &str = "/data";

fn main() {
    let n_threads = get_numthreads_parameter();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build_global()
        .expect("[ERROR] No se pudo iniciar Rayon con la cantidad de threads indicada");

    let json_paths = get_json_paths(DATA_PATH);

    let mut processed_sites = process_sites(json_paths);

    processed_sites.process_chatty();

    let serialized = serde_json::to_string_pretty(&processed_sites)
        .expect("[ERROR] No se pudieron serializar los ProcessedSites a un JSON");
    println!("{}", serialized);
}

/// Obtiene la cantidad de threads a ejecutar indicada por línea de comando. En caso de que se ingrese una cantidad errónea, se va a utilizar la cantidad de threads disponibles en el sistema en el que se está ejecutando.
fn get_numthreads_parameter() -> usize {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && args[1].parse::<usize>().is_ok() {
        // println!("[INFO] La cantidad de threads especificada es {}", args[1]);
        args[1]
            .parse()
            .expect("Ya me fijé que se puede parsear a un usize")
    } else {
        let default_parallelism_approx = available_parallelism()
            .expect("No se pudo obtener la cantidad de threads del sistema")
            .get();
        if args.len() != 1 {
            eprintln!("[ERROR] Parámetros inválidos, se usará el valor adecuado para este sistema ({} threads)", default_parallelism_approx);
        }
        default_parallelism_approx
    }
}
