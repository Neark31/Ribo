use polars::prelude::*;
use std::fs::File;


fn main() -> Result<(), PolarsError>{

    // on recupere le csv string pour notre dataset
    let file_path = "G://Code/Kaggle/Stanford_Ribonanza/sequencesample.csv"; 

    let mut df = CsvReader::from_path(file_path)
    .unwrap()
    .finish()
    .unwrap();


    // on renome la premiere colone crete par l'extract en csv
    let original_name = "";
    let new_name = "mainindex";

    df.rename(original_name, new_name);
    println!("df:{:?}", df);

    // now we create the position vector and the serie that will be added to the df
    let vec_letter_index: Vec<i32> = (0..=530).collect();
    let letter_index = Series::new("lettre_index", vec_letter_index);


    // now lets collect the neededserie into vector
    let series_a = &df["id_min"];
    let id_min: Vec<i64> = series_a.i64()?.into_no_null_iter().collect();

    let series_b = &df["id_max"];
    let id_max: Vec<i64> = series_b.i64()?.into_no_null_iter().collect();


    let series_c = &df["sequence_id"];
    let sequence_id: Vec<&str> = series_c.utf8()?
    .into_iter()
    .filter_map(|opt_str| opt_str) // Filter out the None values
    .collect();

    let series_d = &df["sequence"];
    let sequence: Vec<&str> = series_d.utf8()?
    .into_iter()
    .filter_map(|opt_str| opt_str) // Filter out the None values
    .collect();



    // on a maintenant 4 vecteur du df et le vecteur index de la longeuru finale. 
    //on va passer sur chacun des elements
    let mut count :i64 = 0;
    //let mut a = 0;

    let mut id_min_dup = Vec::new();
    let mut id_max_dup = Vec::new();
    let mut sequence_id_dup = Vec::new();
    let mut sequence_dup = Vec::new();



    for (i, _val) in id_max.iter().enumerate(){

        //println!("id_max[i]:{:?}", id_max[i]);
        while count <= id_max[i]{

            id_min_dup.push(id_min[i]);
            id_max_dup.push(id_max[i]);
            sequence_id_dup.push(sequence_id[i]);
            sequence_dup.push(sequence[i]);

        count +=1;
        } 
   
    }



    //controle de la longeur de chacun des vecteurs
    /* 
    println!("letter_index:{:?}", letter_index.len());
    println!("id_min:{:?}", id_min_dup.len());
    println!("id_max:{:?}", id_max_dup.len());
    println!("sequence_id:{:?}", sequence_id_dup.len());
    println!("sequence:{:?}", sequence_dup.len());
    */


    //maintenant on recree le df qui passera dans notre fonction
    let id_min = Series::new("id_min", id_min_dup);
    let id_max = Series::new("id_max", id_max_dup);
    let sequence_id = Series::new("sequence_id", sequence_id_dup);
    let sequence= Series::new("sequence", sequence_dup);

    
    let mut sorted = DataFrame::new(vec![letter_index, sequence_id, sequence,id_min, id_max])?;
   // println!("sorted:{:?}", sorted);




    


    let chemin = "G://Code/Kaggle/Stanford_Ribonanza/extractsequence.csv";
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

    let chemin = "G://Code/Kaggle/Stanford_Ribonanza/output2.csv";
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
