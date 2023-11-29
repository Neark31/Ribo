use polars::prelude::*;
use std::fs::File;


//use std::io::{self, BufRead, BufReader, Write};



fn main() -> Result<(), PolarsError>{

    // on recupere le csv string pour notre dataset
    let file_path = "G://Code/Kaggle/Stanford_Ribonanza/troissample.csv"; 

    let df = CsvReader::from_path(file_path)
    .unwrap()
    .finish()
    .unwrap();

    //println!("df:{:?}", df);

    let id_vars = vec!["ind", "sequence_id", "sequence", "experiment_type", "dataset_name","reads","signal_to_noise","SN_filter"];
 
    let mut value_vars = Vec::new();

    let input_file = File::open(file_path).expect("Failed to open file");
    let mut rdr = csv::Reader::from_reader(input_file);

    

    let headers = rdr.headers().expect("Failed to read headers");

    let mut count = 0;

    let header_vector: Vec<String> = headers.iter().map(|s| s.to_string()).collect();

    
    for (_index,item) in header_vector.iter().enumerate() {

        if count >7 {

            value_vars.push(item.as_str());

        }
        
        count +=1;        
    }
    

    let melted_df = df.melt(id_vars, value_vars)?;

    //println!("melted_df:{:?}", melted_df);

     // pour trier decroissant
     let _descending = vec![true; 1];

     // pour trier croissant
     let ascending = vec![false; 1];
 
     // sort the dataset ascending
     let mut sorted = melted_df
         .lazy()
         .sort_by_exprs(
             vec![
                 col("sequence_id"),
                 col("*").exclude(vec!["sequence_id"]),
             ],
             ascending,
             false,
             false,
         )
         .collect()?;
    // println!("{:?}", sorted);

     let chemin = "G://Code/Kaggle/Stanford_Ribonanza/extractsample.csv";
    let mut file = std::fs::File::create(chemin).unwrap();
    CsvWriter::new(&mut file).finish(&mut sorted).unwrap();

    //on appele la fonction qui transformera le csf file et le passera dans un nouveau dataframe

    let vectorlettre3 = modif_csv_file(chemin.to_string());
    let vectorlettre = vectorlettre3.0;
    // on convertit le vector de char en en vectore de string pour pouvoir le basculer dans une serie polars
    let string_data: Vec<String> = vectorlettre.iter().collect::<String>().chars().map(|c| c.to_string()).collect();
    //On cree donc un nouvelle serie
    let lettre_serie = Series::new("lettre_serie", string_data);

    let vectorlettreprec = vectorlettre3.1;
    let string_data: Vec<String> = vectorlettreprec.iter().collect::<String>().chars().map(|c| c.to_string()).collect();
    let lettreprec_serie = Series::new("lettre_N-1", string_data);


    let vectorlettresuiv = vectorlettre3.2;
    let string_data: Vec<String> = vectorlettresuiv.iter().collect::<String>().chars().map(|c| c.to_string()).collect();
    let lettresuiv_serie = Series::new("lettre_N+1", string_data);


    let mut df = sorted.with_column(lettre_serie)?.with_column(lettreprec_serie)?.with_column(lettresuiv_serie)?;
    

    println!("df:{:?}", df);

    let chemin = "G://Code/Kaggle/Stanford_Ribonanza/output.csv";
    let mut file = std::fs::File::create(chemin).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    Ok(())
}


//fonction qui modifie le csv file et le retourne
fn modif_csv_file(pt: String) -> (Vec<char>,Vec<char>,Vec<char>)  {

    //on ouvre le fichier - tentont de passer le chemin en argument

    // on passe le checmin dans une variable de tyope "file" pour notre fichier
    let input_file = File::open(pt).expect("Failed to open file");

    //puiq son paase ce fichier dans un csv reader
    // A CSV reader takes as input CSV data and transforms that into standard Rust values
    let mut rdr = csv::Reader::from_reader(input_file);


    // on cree unevecteur pour pouvoir recuperer la lettre
    let mut vecletter = Vec::new();

    //et des vecteur pour la precedente et suivante lettre
    let mut vecletterprev = Vec::new();
    let mut vecletternext = Vec::new();


    //et un vecteur pour manipuler les éléments de chaque ligne
    let mut vec = Vec::new();
    //on passe en String la variable temp, utilsé pour controler la sequence, pour etre capable d'utiliser clone car cela ne marche pas sur un str
    let mut temp = "sss".to_owned();
    //et des variable count pour naviguers sur les elements des string et du record
    let mut count = 0;
    let mut countseq = 0;

    //on loop maintenant sur le fichier
    for result in rdr.records() {

        //on passe la ligne dans une variable
        let record = result.expect("a CSV record");
       
        //on gere le cas du 1er record
        if count == 0 {
            //on le rentrre dans un vecteur
            vec.push(record);

            //on clone la valeur
            let pp = vec[count].clone();

            //on convertit l'element du string record en string pour pouvoir le cloner. sinon avec un str on le borrowed et on aurait un probleme de life esperancy
            let rr = pp[2].to_string();

            // on passe la sequnce en cours dans une variable temporaire en la clonant
            temp = rr.clone();

            //on recupere la premiere lettre de la 1ere sequence
            vecletter.push(rr.chars().nth(0).unwrap());
            // comme premiere lettre alors pas de precedente. on le balise avec f
            vecletterprev.push('f');

            //on incremente le nb de lettre recupere de la sequence.
            countseq += 1;


        //on gere maintenant tous les autres records

        } else {
            // on passe chaque ligne dans le vecteur
            vec.push(record);

            //on accede a la sequnce du record ajouté
            let ppe = &vec[count].clone();

            //on va recuperer la 3eme colonne du vecteur et passer la valeur dans un nouveau vecteur
            let rra = &ppe[2];

            let longeur = rra.len();

            // on regarde si l'on est dans la meme sequence ou non

            //si oui alors on navigue sur la lettre suivante et la recupere
            if &temp == &rra{

                // on s'assure que l'on a pas depasse la longeur de la chaine, sinon on se pousse un n
                if countseq == longeur {

                    vecletterprev.push(vecletter.last().copied().unwrap());
                    vecletter.push('n');
                    vecletternext.push('n');

                    countseq += 1;

                } else if countseq > longeur{

                    vecletterprev.push('n');
                    vecletter.push('n');
                    vecletternext.push('n');

                    countseq += 1;

                } else {

                    vecletterprev.push(vecletter.last().copied().unwrap());

    
                    vecletter.push(rra.chars().nth(countseq).unwrap());
                    vecletternext.push(rra.chars().nth(countseq).unwrap());
    
                    countseq += 1;

                }



            //si non on reinitialise ler count de sequence et on recupere la 1er lettre de la nouvelle sequence
            } else {
                vecletter.push(rra.chars().nth(0).unwrap());
                countseq = 1;
                vecletterprev.push('f');

                // comme derniere lettre alors pas de suivante. on le balise avec f
                vecletternext.push('l');
            }
            //oon rtecupere la nouvelle sequence pour les controles suivants
            temp = rra.to_string().clone();
        }
        //on passe au nouveau record
        count +=1;
    }
    //on finalise ole decalage en balisant le dernier charactere
    vecletternext.push('l');

    //On print notre vecteur de lettre pour verifer
    //println!("{:?}", vecletter);
    //println!("{:?}", vecletterprev);
    //println!("{:?}", vecletternext);

    return (vecletter,vecletterprev,vecletternext)

    

}
