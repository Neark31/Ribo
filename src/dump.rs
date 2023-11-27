
/* 
use polars::prelude::*;

//On cherche a faire un basculement de la table avec pour chque séquence, une ligne correspondra à la postiion dans la sequence et sa reactivité

fn main() {
    // Specify the file path to your CSV file.
    let file_path = "G://Code/Kaggle/Stanford_Ribonanza/datatransform.csv";

    // Read the CSV file into a DataFrame.
    let df = CsvReader::from_path(file_path)
        .unwrap()
        .finish()
        .unwrap();

    // Now you have your data in the DataFrame 'df'.
    println!("{:?}", df);


}

*/