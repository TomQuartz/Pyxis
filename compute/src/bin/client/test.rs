trait UpdateX{
    fn updatex(new_x:)
}

struct Parent{
    a: dummy,
}

struct Child{
    parent: Rc<RefCell<Parent>>,
} 

struct All{
    p: Rc<Parent>,
    c: Child,
}
impl All{
    fn new()->Self{
        let p = Rc::new(Parent{a:dummy{0,0}});
        let c = Child{}
    }
}

struct ServerLoad {
    ip2load: HashMap<u32, Vec<RwLock<MovingAvg>>>,
}

impl ServerLoad {
    fn new(ip_addrs: Vec<u32>, num_cores: u16, exp_decay: f64) -> ServerLoad {
        let mut ip2load = HashMap::new();
        for ip in ip_addrs {
            let mut server_load = vec![];
            for _ in 0..num_cores {
                server_load.push(RwLock::new(MovingAvg::new(exp_decay)));
            }
            ip2load.insert(ip, server_load);
        }
        ServerLoad { ip2load }
    }
    fn update(&self, src_ip: u32, src_port: u16, delta: f64) -> Result<(), ()> {
        if let Some(server_load) = self.ip2idx.get(&src_ip) {
            server_load[src_port as usize]
                .write()
                .unwrap()
                .update(delta);
            Ok(())
        } else {
            Err(())
        }
    }
    fn avg_server(&self, ip: u32) -> f64 {
        let server_load = self.ip2load.get(&ip).unwrap();
        let num_cores = server_load.len() as f64;
        let mut total = 0f64;
        for core_load in server_load.iter() {
            total += core_load.read().unwrap().avg();
        }
        total / num_cores
    }
    fn avg_all(&self) -> f64 {
        let mut total = 0f64;
        let num_servers = self.load.len() as f64;
        for &ip in self.ip2load.keys() {
            total += self.avg_server(ip);
        }
        total / num_servers
    }
}