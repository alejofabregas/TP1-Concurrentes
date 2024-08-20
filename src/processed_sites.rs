use std::{
    collections::HashMap,
    fs::{read_dir, File},
    io::{BufRead, BufReader},
    path::PathBuf,
};

use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use serde::Serialize;

use crate::{line::Line, site::Site, tag::Tag};

const PADRON: &str = "106160";

/// Estructura que contiene la información final del cómputo que se va a mostrar en formato JSON. Para eso, serializamos con serde_json.
#[derive(Debug, Serialize)]
pub struct ProcessedSites {
    pub padron: String,
    pub sites: HashMap<String, Site>,
    pub tags: HashMap<String, Tag>,
    pub totals: HashMap<String, Vec<String>>,
}

impl ProcessedSites {
    /// Crea el ProcessedSites con la información que fue calculada anteriormente.
    pub fn new(
        padron: String,
        sites: HashMap<String, Site>,
        tags: HashMap<String, Tag>,
        totals: HashMap<String, Vec<String>>,
    ) -> ProcessedSites {
        ProcessedSites {
            padron,
            sites,
            tags,
            totals,
        }
    }

    /// Genera todos los chattys (top 10) para este ProcessedSites.
    /// Calcula los chatty_sites.
    /// Calcula los chatty_tags para cada Site.
    /// Calcula los chatty_tags de los Tags totales.
    pub fn process_chatty(&mut self) {
        let chatty_sites_totals: Vec<(&String, f64)> = self
            .sites
            .par_iter()
            .map(|(name, site)| (name, site.words as f64 / site.questions as f64))
            .collect();
        self.totals
            .insert("chatty_sites".to_string(), get_chatty(chatty_sites_totals));

        let chatty_tags_totals: Vec<(&String, f64)> = self
            .tags
            .par_iter()
            .map(|(name, tag)| (name, tag.words as f64 / tag.questions as f64))
            .collect();
        self.totals
            .insert("chatty_tags".to_string(), get_chatty(chatty_tags_totals));

        self.sites.iter_mut().for_each(|(_site_name, site)| {
            let chatty_tags: Vec<(&String, f64)> = site
                .tags
                .par_iter()
                .map(|(name, tag)| (name, tag.words as f64 / tag.questions as f64))
                .collect();
            site.chatty_tags.extend(get_chatty(chatty_tags));
        });
    }
}

/// A partir de un vector de items del tipo (string, ratio words/questions),
/// devuelve un vector con las strings chatty (top 10 con mayor ratio words/questions).
/// Funciona para chatty_sites y chatty_tags.
fn get_chatty(mut chatty_items: Vec<(&String, f64)>) -> Vec<String> {
    chatty_items.sort_by(|item_1, item_2| match (item_2.1).total_cmp(&(item_1.1)) {
        std::cmp::Ordering::Equal => item_1.0.cmp(item_2.0),
        other => other,
    });
    if chatty_items.len() > 10 {
        chatty_items = chatty_items[0..10].to_vec();
    }
    chatty_items
        .iter()
        .map(|(tag_name, _tag)| tag_name.to_string())
        .collect()
}

/// Obtiene los paths de los archivos JSON del directorio indicado por parámetro.
pub fn get_json_paths(path: &str) -> Vec<PathBuf> {
    read_dir(format!("{}{}", env!("CARGO_MANIFEST_DIR"), path))
        .expect("[ERROR] No se pudieron obtener los paths de los archivos JSON a procesar.")
        .flatten()
        .map(|d| d.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "jsonl"))
        .collect::<Vec<PathBuf>>()
}

/// Lee los archivos JSON pasados por parámetro y los va procesando concurrentemente línea por línea para obtener el conjunto de Sites procesados, junto con sus Tags. No se procesan los chatty_tags ni los totals.
/// Se crean por cada línea objetos de tipo ProcessedSites en el map, y se van uniendo de a pares en el reduce.
/// El resultado es un ProcessedSites que tiene tantos Sites como archivos JSON haya.
pub fn process_sites(json_paths: Vec<PathBuf>) -> ProcessedSites {
    let processed_sites = json_paths
        .par_iter()
        .flat_map(|path| {
            let file = File::open(path);
            let reader = BufReader::new(file.expect("[ERROR] No se pudo leer el archivo"));
            let sitename = path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .replace(".jsonl", "");
            reader
                .lines()
                .map(move |l| (sitename.clone(), l))
                .par_bridge()
        })
        .map(|(filename, line)| {
            let line_data: Line =
                serde_json::from_str(&line.expect("[ERRROR] No se pudo leer la línea"))
                    .expect("[ERRROR] No se pudo parsear la línea JSON a un struct Line");
            let full_text = line_data.texts.join(" ");
            let words = full_text.split_whitespace().count();
            let mut tags = HashMap::new();
            for tag in line_data.tags {
                tags.insert(tag, Tag::new(1, words));
            }
            let chatty_tags = vec![];
            let site = Site::new(1, words, tags, chatty_tags);
            let mut hash_site: HashMap<String, Site> = HashMap::new();
            hash_site.insert(filename, site);
            ProcessedSites::new(
                PADRON.to_string(),
                hash_site,
                HashMap::new(),
                HashMap::new(),
            )
        })
        .reduce(
            || {
                ProcessedSites::new(
                    PADRON.to_string(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                )
            },
            |mut total_sites, mut processed_sites| {
                processed_sites
                    .sites
                    .iter_mut()
                    .for_each(|(site_name, site)| {
                        total_sites
                            .sites
                            .entry(site_name.to_string())
                            .and_modify(|s| s.add(site))
                            .or_insert(site.clone());
                        site.tags.iter().for_each(|(tag_name, tag)| {
                            total_sites
                                .tags
                                .entry(tag_name.to_string())
                                .and_modify(|t| t.add(tag))
                                .or_insert(tag.clone());
                        });
                    });
                total_sites
            },
        );
    processed_sites
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn create_processed_sites_correctly() {
        let site1 = Site::new(
            2,
            10,
            HashMap::from([("tag1".to_string(), Tag::new(2, 10))]),
            vec!["chatty_1".to_string()],
        );
        let sites = HashMap::from([("site1".to_string(), site1)]);

        let tag1 = Tag::new(2, 10);
        let tags = HashMap::from([("tag1".to_string(), tag1)]);

        let totals = HashMap::from([
            ("chatty_sites".to_string(), vec!["site1".to_string()]),
            ("chatty_tags".to_string(), vec!["tag1".to_string()]),
        ]);

        let processed_sites = ProcessedSites::new("106160".to_string(), sites, tags, totals);

        assert_eq!(processed_sites.sites.get("site1").unwrap().questions, 2);
        assert_eq!(processed_sites.sites.get("site1").unwrap().words, 10);
        assert_eq!(processed_sites.tags.get("tag1").unwrap().questions, 2);
        assert_eq!(processed_sites.tags.get("tag1").unwrap().words, 10);
        assert_eq!(
            processed_sites.totals.get("chatty_sites").unwrap()[0],
            "site1".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_tags").unwrap()[0],
            "tag1".to_string()
        );
    }

    #[test]
    fn process_chatty_correctly() {
        let site1 = Site::new(
            2,
            10,
            HashMap::from([
                ("tag1".to_string(), Tag::new(2, 10)),
                ("tag2".to_string(), Tag::new(1, 25)),
            ]),
            vec!["chatty_1".to_string()],
        );
        let site2 = Site::new(
            1,
            25,
            HashMap::from([
                ("tag1".to_string(), Tag::new(3, 30)),
                ("tag2".to_string(), Tag::new(4, 4)),
            ]),
            vec!["chatty_2".to_string()],
        );
        let sites = HashMap::from([("site1".to_string(), site1), ("site2".to_string(), site2)]);

        let tag1 = Tag::new(2, 10);
        let tag2 = Tag::new(1, 25);
        let tag3 = Tag::new(3, 30);
        let tag4 = Tag::new(4, 4);
        let tags = HashMap::from([
            ("tag1".to_string(), tag1),
            ("tag2".to_string(), tag2),
            ("tag3".to_string(), tag3),
            ("tag4".to_string(), tag4),
        ]);

        let totals = HashMap::new();

        let mut processed_sites = ProcessedSites::new("106160".to_string(), sites, tags, totals);

        processed_sites.process_chatty();

        assert_eq!(
            processed_sites.totals.get("chatty_sites").unwrap()[0],
            "site2".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_sites").unwrap()[1],
            "site1".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_tags").unwrap()[0],
            "tag2".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_tags").unwrap()[1],
            "tag3".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_tags").unwrap()[2],
            "tag1".to_string()
        );
        assert_eq!(
            processed_sites.totals.get("chatty_tags").unwrap()[3],
            "tag4".to_string()
        );
    }

    #[test]
    fn get_chatty_correctly() {
        let num1 = "num1".to_string();
        let num2 = "num2".to_string();
        let num3 = "num3".to_string();
        let num4 = "num4".to_string();
        let num5 = "num5".to_string();
        let num6 = "num6".to_string();
        let num7 = "num7".to_string();
        let num8 = "num8".to_string();
        let num9 = "num9".to_string();
        let num10 = "num10".to_string();
        let num11 = "num11".to_string();
        let num12 = "num12".to_string();
        let num13 = "num13".to_string();
        let num14 = "num14".to_string();
        let num15 = "num15".to_string();

        let items = vec![
            (&num1, 58.5980452027),
            (&num2, 94.7379952794),
            (&num3, 51.1235244736),
            (&num4, 66.0260148323),
            (&num5, 8.1785348445),
            (&num6, 56.7970283287),
            (&num7, 76.515530613),
            (&num8, 13.00056099),
            (&num9, 56.8006524708),
            (&num10, 49.57237619),
            (&num11, 58.6966479713),
            (&num12, 48.0282510822),
            (&num13, 87.699566394),
            (&num14, 66.862834759),
            (&num15, 60.4379047171),
        ];

        let _ordered_items = vec![
            (&num2, 94.7379952794),
            (&num13, 87.699566394),
            (&num7, 76.515530613),
            (&num14, 66.862834759),
            (&num4, 66.0260148323),
            (&num15, 60.4379047171),
            (&num11, 58.6966479713),
            (&num1, 58.5980452027),
            (&num9, 56.8006524708),
            (&num6, 56.7970283287),
        ];

        let result = get_chatty(items);

        let correct_result = vec![
            "num2".to_string(),
            "num13".to_string(),
            "num7".to_string(),
            "num14".to_string(),
            "num4".to_string(),
            "num15".to_string(),
            "num11".to_string(),
            "num1".to_string(),
            "num9".to_string(),
            "num6".to_string(),
        ];

        assert_eq!(result, correct_result);
    }

    #[test]
    fn get_correct_sites() {
        let json_paths = get_json_paths("/test_data");

        let mut processed_sites = process_sites(json_paths);

        processed_sites.process_chatty();

        let site_academia = processed_sites
            .sites
            .get("academia.stackexchange.com")
            .unwrap();
        let tag_computer_science = site_academia.tags.get("computer-science").unwrap();

        assert_eq!(site_academia.questions, 5);
        assert_eq!(site_academia.words, 830);
        assert_eq!(site_academia.tags.len(), 13);
        assert_eq!(tag_computer_science.questions, 2);
        assert_eq!(tag_computer_science.words, 347);
    }

    #[test]
    fn get_correct_total_tags() {
        let json_paths = get_json_paths("/test_data");

        let mut processed_sites = process_sites(json_paths);

        processed_sites.process_chatty();

        let tag_computer_science = processed_sites.tags.get("computer-science").unwrap();

        assert_eq!(tag_computer_science.questions, 3);
        assert_eq!(tag_computer_science.words, 439);

        assert_eq!(processed_sites.tags.len(), 38);
    }

    #[test]
    fn get_correct_chatty_tags_chatty_sites() {
        let json_paths = get_json_paths("/test_data");

        let mut processed_sites = process_sites(json_paths);

        processed_sites.process_chatty();

        let chatty_sites = processed_sites.totals.get("chatty_sites").unwrap();
        assert_eq!(
            *chatty_sites,
            [
                "academia.stackexchange.com".to_string(),
                "android.stackexchange.com".to_string(),
                "anime.stackexchange.com".to_string()
            ]
        );

        let chatty_tags = processed_sites.totals.get("chatty_tags").unwrap();
        assert_eq!(
            *chatty_tags,
            [
                "career-path".to_string(),
                "bind-mount".to_string(),
                "mount".to_string(),
                "partitions".to_string(),
                "permissions".to_string(),
                "storage".to_string(),
                "application".to_string(),
                "graduate-admissions".to_string(),
                "recommendation-letter".to_string(),
                "2.2-froyo".to_string()
            ]
        );
    }

    #[test]
    #[ignore]
    fn processes_faster_with_more_threads() {
        // Process with 1 thread
        let start_1_thread = Instant::now();
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build_scoped(
                |thread| thread.run(),
                |pool| {
                    pool.install(|| {
                        let json_paths1 = get_json_paths("/data");
                        let mut processed_sites1 = process_sites(json_paths1);
                        processed_sites1.process_chatty();
                    })
                },
            )
            .expect("[ERROR] No se pudo iniciar Rayon con la cantidad de threads indicada");
        let time_1_thread = start_1_thread.elapsed();

        // Process with 4 threads
        let start_4_threads = Instant::now();
        rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build_scoped(
                |thread| thread.run(),
                |pool| {
                    pool.install(|| {
                        let json_paths4 = get_json_paths("/data");
                        let mut processed_sites4 = process_sites(json_paths4);
                        processed_sites4.process_chatty();
                    })
                },
            )
            .expect("[ERROR] No se pudo iniciar Rayon con la cantidad de threads indicada");
        let time_4_threads = start_4_threads.elapsed();

        assert!(time_4_threads < time_1_thread);
    }

    #[test]
    fn same_output_with_more_threads() {
        // Process with 1 thread
        let threadpool_1_thread = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("[ERROR] No se pudo iniciar Rayon con la cantidad de threads indicada");
        let json_paths1 = get_json_paths("/test_data");
        let mut processed_sites1 = process_sites(json_paths1);
        processed_sites1.process_chatty();
        drop(threadpool_1_thread);

        // Process with 4 threads
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .expect("[ERROR] No se pudo iniciar Rayon con la cantidad de threads indicada");
        let json_paths4 = get_json_paths("/test_data");
        let mut processed_sites4 = process_sites(json_paths4);
        processed_sites4.process_chatty();

        let totals_1_thread = processed_sites1.totals;
        let totals_4_threads = processed_sites4.totals;

        assert_eq!(totals_1_thread, totals_4_threads);
    }
}
