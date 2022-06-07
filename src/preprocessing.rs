use regex::Regex;
use lazy_static::lazy_static;

// Split a raw sql query into a template and parameters
// A query template contains only the operations but no values
// All parameters are connected as a string by comma
pub fn split_query(query: &str) -> (String, String) {
    lazy_static! {
        static ref RULES: Vec<&'static str> = vec![
            r#"('\d+\\.*?')"#,                // hash values
            r#"'((')|(.*?([^\\])'))"#,        // string
            r#""((")|(.*?([^\\])"))"#,        // double-quoted string
            r#"([^a-zA-Z'(,\*])\d+(\.\d+)?"#,   // integers(prevent us from capturing table name like "a1")
        ];
        static ref REGEX_SETS: Vec<Regex> = RULES.iter()
                    .map(|s| Regex::new(s).unwrap())
                    .collect();
    }

    let mut bitmap = vec![0;query.len()];
    let mut indice_pairs = Vec::new();
    for re in REGEX_SETS.iter() {
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
    for re in REGEX_SETS.iter() {
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
    if parameters.is_empty() { return template; }

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
    fn test1() {
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

    #[test]
    fn test2() {
        let query = "INSERT INTO m.avl  (agency_id, trip_id, route_id, avl_lat, avl_lon, vehicle_id, destination_id, avl_time_hour, \
            avl_time_minute, avl_date, route_short_name, block_ref) VALUES (80, '33\\3aaecf2b3ea22a0169d3f9ca8e57150f', \
            '3\\5af586f8860571f3b29eb439e2985fb0',40.649485,-73.916274,6623,307182,21,13,'2016-11-29', \
            '3\\5af586f8860571f3b29eb439e2985fb0', '46\\875cccffb1923728f3269e9eb5400ed7')";

        let (template, parameters) = split_query(query);
        assert_eq!(template, "INSERT INTO m.avl  (agency_id, trip_id, route_id, avl_lat, avl_lon, vehicle_id, destination_id, avl_time_hour, \
            avl_time_minute, avl_date, route_short_name, block_ref) VALUES (@, @, @,@,@,@,@,@,@,@, @, @)");
        assert_eq!(parameters, "80,'33\\3aaecf2b3ea22a0169d3f9ca8e57150f','3\\5af586f8860571f3b29eb439e2985fb0',40.649485,\
            -73.916274,6623,307182,21,13,'2016-11-29','3\\5af586f8860571f3b29eb439e2985fb0','46\\875cccffb1923728f3269e9eb5400ed7'");

        let restored_query = merge_query(template, parameters);
        assert_eq!(restored_query, query);
    }

    #[test]
    fn test3() {
        let query = "SELECT ost.estimate_source, r.route_id, r.route_short_name, r.route_long_name,os.stop_name, t.trip_id \
            AS trip_id, t.trip_headsign AS trip_headsign, t.direction_id,  (ost.departure_time_hour*60+ost.departure_time_minute - $1) \
            AS departure_time_relative, ceiling((h.distance(40.441012,-79.877991,os.stop_lat,os.stop_lon)/1.29)/60) AS walk_time,  \
            CASE ost.fullness IS NULL WHEN true THEN -1.0 ELSE ost.fullness END FROM m.stop os, m.route r, m.trip t, \
            m.stop_time ost, m.calendar c WHERE os.stop_id = $2  AND os.agency_id = $3  AND r.agency_id = os.agency_id  \
            AND t.agency_id = os.agency_id  AND ost.agency_id = os.agency_id  AND c.agency_id = os.agency_id  \
            AND os.stop_id = ost.stop_id  AND t.trip_id = ost.trip_id  AND r.route_id = t.route_id  \
            AND (ost.departure_time_hour*60*60 + ost.departure_time_minute*60) >= $4  \
            AND (ost.departure_time_hour*60*60 + ost.departure_time_minute*60) <= $5  AND c.saturday = 1  \
            AND c.service_id = t.service_id  UNION SELECT ost.estimate_source, r.route_id, r.route_short_name, \
            r.route_long_name,os.stop_name, t.trip_id AS trip_id, t.trip_headsign AS trip_headsign, t.direction_id,  \
            (ost.departure_time_hour*60+ost.departure_time_minute - $6)  AS departure_time_relative, \
            ceiling((h.distance(40.441012,-79.877991,os.stop_lat,os.stop_lon)/1.29)/60) AS walk_time, \
             CASE ost.fullness IS NULL WHEN true THEN -1.0 ELSE ost.fullness END FROM m.stop os, m.route r, \
             m.trip t, m.stop_time ost, m.calendar_date cd  WHERE os.stop_id = $7  AND os.agency_id = $8  \
             AND r.agency_id = os.agency_id  AND t.agency_id = os.agency_id  AND ost.agency_id = os.agency_id  \
             AND cd.agency_id = os.agency_id  AND os.stop_id = ost.stop_id  AND t.trip_id = ost.trip_id  \
             AND r.route_id = t.route_id  AND (ost.departure_time_hour*60*60 + ost.departure_time_minute*60) >= $9  \
             AND (ost.departure_time_hour*60*60 + ost.departure_time_minute*60) <= $10  AND t.service_id = cd.service_id  \
             AND cd.exception_date = $11  AND cd.exception_type = 1  )  EXCEPT SELECT ost.estimate_source, r.route_id, \
             r.route_short_name, r.route_long_name,os.stop_name, t.trip_id AS trip_id, t.trip_headsign AS trip_headsign, \
             t.direction_id,  (ost.departure_time_hour*60+ost.departure_time_minute - $12)  \
             AS departure_time_relative, ceiling((h.distance(40.441012,-79.877991,os.stop_lat,os.stop_lon)/1.29)/60) \
             AS walk_time,  CASE ost.fullness IS NULL WHEN true THEN -1.0 ELSE ost.fullness END FROM m.stop os, \
             m.route r, m.trip t, m.stop_time ost, m.calendar_date cd WHERE os.stop_id = $13  AND os.agency_id = $14  \
             AND r.agency_id = os.agency_id  AND t.agency_id = os.agency_id  AND ost.agency_id = os.agency_id  \
             AND cd.agency_id = os.agency_id  AND os.stop_id = ost.stop_id  AND t.trip_id = ost.trip_id  \
             AND r.route_id = t.route_id  AND t.service_id = cd.service_id  AND cd.exception_date = $15  \
             AND cd.exception_type = 2  ORDER BY departure_time_relative ASC ";


        let (template, parameters) = split_query(query);
        let restored_query = merge_query(template, parameters);
        assert_eq!(restored_query, query);
    }
}