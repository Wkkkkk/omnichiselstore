use omnichiselstore::boost::*;

#[tokio::test(flavor = "multi_thread")]
async fn connect_to_cluster() {
    let mut replicas = setup_replicas(2).await;

    // run test
    tokio::task::spawn(async {
        let res = query(1, String::from("SELECT 1+1;")).await.unwrap();

        assert!(res == "2");
    }).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_read() {
    // create 2 replicas
    let mut replicas = setup_replicas(2).await;

    // run test
    tokio::task::spawn(async {
        // create table
        query(1, String::from("CREATE TABLE IF NOT EXISTS test (id integer PRIMARY KEY)")).await.unwrap();

        
        // create new entry
        query(1, String::from("INSERT INTO test VALUES(1)")).await.unwrap();
        
        // read new entry from replica 1
        let res = query(1, String::from("SELECT id FROM test WHERE id = 1")).await.unwrap();
        assert!(res == "1");
        log(format!("query res: {}", res).to_string());
        
        // read new entry from replica 2
        let res = query(2, String::from("SELECT id FROM test WHERE id = 1")).await.unwrap();
        assert!(res == "1");
        log(format!("query res: {}", res).to_string());

        // drop table
        query(1, String::from("DROP TABLE test")).await.unwrap();
    }).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn synchronous_writes() {
    // ChiselStore uses SQLite which only allows for synchronous writes
    // 1. write (1,1) -> replica A
    // 2. (over-)write (1,2) -> replica B
    // 3. read x from replica A
    // 4. read y from replica B
    // 5. x == y
  
    // create 2 replicas
    let mut replicas = setup_replicas(2).await;

    // START: test

    // create table
    tokio::task::spawn(async {
        query(1, String::from("CREATE TABLE IF NOT EXISTS test_synchronous (id integer PRIMARY KEY, value integer NOT NULL)")).await.unwrap();
    }).await.unwrap();
    
    // write replica A
    let write_a = tokio::task::spawn(async {
        // create new entry
        println!("write_a");
        query(1, String::from("INSERT OR REPLACE INTO test_synchronous VALUES(1,1)")).await.unwrap();
    });
    
    // write replica B
    let write_b = tokio::task::spawn(async {
        // create new entry
        println!("write_b");
        query(2, String::from("INSERT OR REPLACE INTO test_synchronous VALUES(1,2)")).await.unwrap();
    });
    
    write_a.await.unwrap();
    write_b.await.unwrap();

    // read new entry from replica 1
    let x = query(1, String::from("SELECT value FROM test_synchronous WHERE id = 1")).await.unwrap();
    
    // read new entry from replica 2
    let y = query(2, String::from("SELECT value FROM test_synchronous WHERE id = 1")).await.unwrap();
    
    assert!(x == y);

    // END: TEST
    
    // drop table
    query(1, String::from("DROP TABLE test_synchronous")).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_crashes() {
    /// Note: LittleRaft does not support changes to the cluster and will get stuck
    // Write to cluster, kill leader, read written value from another node
    
    let mut replicas = setup_replicas(4).await;

    // START: test
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // wait for leader election

    let mut leader_idx = 0;
    for r in replicas.iter() {
        if r.is_leader() {
            leader_idx = r.get_id();
            break
        }
    }    
    log(format!("Leader is node {}", leader_idx).to_string());

    // create table
    tokio::task::spawn(async move {
        query(leader_idx, String::from("CREATE TABLE IF NOT EXISTS test_leader_drop (id integer PRIMARY KEY)")).await.unwrap();
        query(leader_idx, String::from("CREATE TABLE IF NOT EXISTS test_leader_drop (id integer PRIMARY KEY)")).await.unwrap();
        query(leader_idx, String::from("CREATE TABLE IF NOT EXISTS test_leader_drop (id integer PRIMARY KEY)")).await.unwrap();
        query(leader_idx, String::from("CREATE TABLE IF NOT EXISTS test_leader_drop (id integer PRIMARY KEY)")).await.unwrap();
    }).await.unwrap();

    // kill leader
    let leader = replicas.remove(leader_idx as usize);
    leader.shutdown().await;

    log(format!("Leader with idx {} dead", leader_idx).to_string());
    
    // reconfigure
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // wait for leader election
    
    let mut new_leader_idx = 0;
    let mut new_cluster: Vec<u64> = Vec::new();
    for (i, r) in replicas.iter().enumerate() {
        if r.is_leader() {
            new_leader_idx = i;
        }
        new_cluster.push(r.get_id());
    }

    replicas[new_leader_idx].reconfigure(new_cluster);

    log(format!("Leader with idx {} reconfigure", new_leader_idx).to_string());
    
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await; // wait for reconfiguration
    
    // continue querying
    let living_replica_id = replicas[new_leader_idx].get_id();
    
    // write to table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("INSERT INTO test_leader_drop VALUES(1)")).await.unwrap();
    }).await.unwrap();
    
    // END: Test
    
    // drop table
    let living_replica_id = replicas[new_leader_idx].get_id();
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("DROP TABLE test_leader_drop")).await.unwrap();
    }).await.unwrap();
    
    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_crashes() {
    // Write to cluster, kill follower, read written value from another node
    /// Note: LittleRaft does not support changes to the cluster and will get stuck
    
    let mut replicas = setup_replicas(3).await;

    // START: test

    // create table
    tokio::task::spawn(async {
        query(1, String::from("CREATE TABLE IF NOT EXISTS test_follower_drop (id integer PRIMARY KEY)")).await.unwrap();
    }).await.unwrap();
    
    // kill a follower
    let mut follower_idx = 0;
    for (i, r) in replicas.iter().enumerate() {
        if !r.is_leader() {
            follower_idx = i;
            break
        }
    }

    let follower = replicas.remove(follower_idx);
    follower.shutdown().await;

    let living_replica_id = replicas[0].get_id();
    
    // write to table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("INSERT INTO test_follower_drop VALUES(1)")).await.unwrap();
    }).await.unwrap();

    // END: Test

    // drop table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("DROP TABLE test_follower_drop")).await.unwrap();
    }).await.unwrap();
    
    shutdown_replicas(replicas).await;
}