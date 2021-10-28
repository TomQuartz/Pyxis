use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
extern crate atomic_float;
use self::atomic_float::AtomicF32;

// fn main() {
//     let mut v = vec![];
//     for _ in 0..5{
//         v.push(Arc::new(AtomicUsize::new(0)));
//     }
//     let mut pool = vec![];
//     for i in 0..5 {
//         let vc = v.clone();
//         pool.push(thread::spawn(move || {
//             vc[i].fetch_add(i, Ordering::SeqCst);
//             println!("{:?}", vc);
            
//         }));
//     }
//     for t in pool {
//         t.join();
//     }
// }

// fn copy_vec(v: &Vec<i32>){
//     let vc = v.clone();
//     println!("copied vec {:?}",vc);
// }

// fn main(){
//     let v = vec![1,2,3];
//     copy_vec(&v);
//     let f = AtomicF32::new(1.0);
//     f.store(2.0,Ordering::Release);
//     println!("{:?}",f);
// }

fn main(){
    let mut vv = vec![vec![1,2],vec![3,4]];
    let vc = vv.clone();
    for v in vv.iter_mut(){
        v.push(5);
    }
    println!("{:?} {:?}",vv,vc);
    // let mut v = vec![0;5];
    // for (i,x) in v.iter_mut().enumerate(){
    //     *x+=i;
    // }
    // println!("{:?}",v);
}
