use regex::Regex;
use lazy_static::lazy_static;

// Split a raw sql query into a template and parameters
// A query template contains only the operations but no values
// All parameters are connected as a string by comma
pub fn split_query(query: &str) -> (String, String) {
    lazy_static! {
        static ref rules: Vec<&'static str> = vec![
            r#"('\d+\\.*?')"#,                // hash values
            r#"'((')|(.*?([^\\])'))"#,        // string
            r#""((")|(.*?([^\\])"))"#,        // double-quoted string
            r#"([^a-zA-Z '(])\d+(\.\d+)?"#,   // integers(prevent us from capturing table name like "a1")
        ];
        static ref regex_sets: Vec<Regex> = rules.iter()
                    .map(|s| Regex::new(s).unwrap())
                    .collect();
    }

    let mut bitmap = vec![0;query.len()];
    let mut indice_pairs = Vec::new();
    for re in regex_sets.iter() {
        for (index, mat) in query.match_indices(re) {
            if bitmap[index] == 1 {
                continue
            } else {
                for i in index..index+mat.len() {
                    bitmap[i] = 1;
                }
            }
    
            indice_pairs.push((index, mat));
        } 
    }
    let mut template = query.to_string();
    for re in regex_sets.iter() {
        template = re.replace_all(&template, "@").to_string();
    }

    indice_pairs.sort_by_key(|p| p.0);
    // println!("indice_pairs: {:?}", indice_pairs);

    // println!("template: {:?}", template);
    let parameters = indice_pairs.iter()
                        .map(|p| p.1)
                        .collect::<Vec<_>>()
                        .join(",");
    // println!("parameters: {:?}", parameters);
    
    (template, parameters)
}

// Merge template string with parameters
// There should be the exact number of parameters to fill in
pub fn merge_query(template: String, parameters: String) -> String {
    let parameter_list = parameters
        .split(",")
        .collect::<Vec<_>>();
    let num_parameters = parameter_list.len();

    let parts = template.split("@").collect::<Vec<_>>();
    assert!(parts.len() == num_parameters+1, "Unmatched templates {} \n and parameters {}", template, parameters);

    let mut query = String::new();
    for i in 0..num_parameters {
        query.push_str(parts[i]);
        query.push_str(parameter_list[i]);
    }
    query.push_str(parts[num_parameters]);

    query
}

#[cfg(test)]
mod tests {
    use crate::preprocessing::*;

    #[test]
    fn it_works() {
        let query = "SELECT st.agency_id, st.id, st.trip_id, st.stop_sequence, e.intercept_time - h.seconds(21, 13, '49') \
        FROM m.estimate e, m.stop_time st WHERE st.trip_id =  '33\\de1c23ef00193a150490901e26c91ea4' \
        AND st.stop_id = '802017' AND e.stop_time_id = st.id AND e.estimate_source =  '8\\32e67cabf78d1bd875e0878d67001805' \
        AND st.agency_id = 80 AND e.agency_id = st.agency_id;";

        let (template, parameters) = split_query(query);
        assert_eq!(template, "SELECT st.agency_id, st.id, st.trip_id, st.stop_sequence, e.intercept_time - h.seconds(@, @, @) \
            FROM m.estimate e, m.stop_time st WHERE st.trip_id =  @ \
            AND st.stop_id = @ AND e.stop_time_id = st.id AND e.estimate_source =  @ \
            AND st.agency_id = @ AND e.agency_id = st.agency_id;");
        assert_eq!(parameters, "21,13,'49','33\\de1c23ef00193a150490901e26c91ea4','802017','8\\32e67cabf78d1bd875e0878d67001805',80");

        let restored_query = merge_query(template, parameters);
        assert_eq!(restored_query, query);
    }
}