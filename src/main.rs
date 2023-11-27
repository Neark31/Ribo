use polars::prelude::*;
use std::fs::File;
//use std::io::{self, BufRead, BufReader, Write};



fn main() -> Result<(), PolarsError>{

    // Create a sample DataFrame with 10 fields
    let df = DataFrame::new(
        vec![
            Series::new("Sequence_ID", &[1, 2, 3]),
            Series::new("Sequence", &["GGGAA", "CGCAA", "GCGAU"]),
            Series::new("experiment", &["AB", "AB", "CD"]),
            Series::new("dataset", &["De", "Pe", "RT"]),
            Series::new("number_field1", &[1, 6, 11]),
            Series::new("number_field2", &[2, 7, 12]),
            Series::new("number_field3", &[3, 8, 13]),
            Series::new("number_field4", &[4, 9, 14]),
            Series::new("number_field5", &[5, 10, 15]),
        ]
    )?;

 

    // Specify the columns you want to keep as-is (text fields)
    let id_vars = vec!["Sequence_ID","Sequence", "experiment", "dataset"];

    // Specify the columns you want to melt (number fields) - melt is the method to unpivot
    let value_vars = vec!["number_field1", "number_field2", "number_field3", "number_field4", "number_field5"];

    // Perform the melt operation to unpivot the DataFrame
    let melted_df = df.melt(id_vars, value_vars)?;
    //println!("{:?}", melted_df);

    // pour trier decroissant
    let _descending = vec![true; 1];

    // pour trier croissant
    let ascending = vec![false; 1];

    // sort the dataset ascending
    let mut sorted = melted_df
        .lazy()
        .sort_by_exprs(
            vec![
                col("Sequence_ID"),
                col("*").exclude(vec!["Sequence_ID"]),
            ],
            ascending,
            false,
            false,
        )
        .collect()?;
    println!("{:?}", sorted);

//il faudra aussi trouver une facon de selectionner seulement les colonnes que l'on veut dans le dataset

    // Save the DataFrame to a CSV file
    let chemin = "G://Code/Kaggle/Stanford_Ribonanza/extracttemp.csv";
    let mut file = std::fs::File::create(chemin).unwrap();
    CsvWriter::new(&mut file).finish(&mut sorted).unwrap();

    //on appele la fonction qui transformera le csf file et le passera dans un nouveau dataframe
    let df = CsvReader::from_path(modif_csv_file(chemin.to_string()))
    .unwrap()
    .finish()
    .unwrap();

    //println!("df:{:?}", df);

    Ok(())
}


//fonction qui modifie le csv file et le retourne
fn modif_csv_file(pt: String) -> String  {

    //on ouvre le fichier - tentont de passer le chemin en argument

    // on passe le checmin dans une variable de tyope "file" pour notre fichier
    let input_file = File::open(pt).expect("Failed to open file");

    //puiq son paase ce fichier dans un csv reader
    // A CSV reader takes as input CSV data and transforms that into standard Rust values
    let mut rdr = csv::Reader::from_reader(input_file);


    // on cree unevecteur pour pouvoir recuperer la lettre
    let mut vecletter = Vec::new();
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
            let rr = pp[1].to_string();

            // on passe la sequnce en cours dans une variable temporaire en la clonant
            temp = rr.clone();

            //on recupere la premiere lettre de la 1ere sequence
            vecletter.push(rr.chars().nth(0).unwrap());

            //on incremente le nb de lettre recupere de la sequence.
            countseq += 1;


        //on gere maintenant tous les autres records

        } else {
            // on passe chaque ligne dans le vecteur
            vec.push(record);

            //on accede a la sequnce du record ajouté
            let ppe = &vec[count].clone();

            //on va recuperer la 2eme colonne du vecteur et passer la valeur dans un nouveau vecteur
            let rra = &ppe[1];

            // on regarde si l'on est dans la meme sequence ou non

            //si oui alors on navigue sur la lettre suivante et la recupere
            if &temp == &rra{
                vecletter.push(rra.chars().nth(countseq).unwrap());
                countseq += 1;

            //si non on reinitialise ler count de sequence et on recupere la 1er lettre de la nouvelle sequence
            } else {
                vecletter.push(rra.chars().nth(0).unwrap());
                countseq = 1;
            }
            //oon rtecupere la nouvelle sequence pour les controles suivants
            temp = rra.to_string().clone();
        }
        //on passe au nouveau record
        count +=1;
    }

    //On print notre vecteur de lettre pour verifer
    println!("{:?}", vecletter);

   
   //une fois notre vecteur avec lettre, on voit si on peut l'ajouter a notre fichier csv

    //ACTION A ECRIRE
    
    //on sauvegarde le fichier sous un nouveau nom ou ecrase la variable precedente

    // on renvoie un path sous forme de string avec le nouvau fichier
    let file_path = "G://Code/Kaggle/Stanford_Ribonanza/extracttemp.csv";    
    return file_path.to_string()

}
