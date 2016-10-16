#!/bin/sh
export RUST_BACKTRACE=1
export RUST_LOG=weinsa_westem_distributedsystems_project1=debug
pkill weinsa_westem_d # pkill has a 15-char length limit for some reason
#cargo build && (for i in $(seq 1 6); do (./target/debug/weinsa_westem_distributedsystems_project1 $i | tee log$i &); done)
cargo build --release && (for i in $(seq 1 6); do (./target/release/weinsa_westem_distributedsystems_project1 $i | tee log$i &); done)
