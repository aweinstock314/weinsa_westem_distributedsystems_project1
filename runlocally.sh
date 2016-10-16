#!/bin/sh
pkill weinsa_westem_d # pkill has a 15-char length limit for some reason
#cargo build && (for i in $(seq 1 6); do (./target/debug/weinsa_westem_distributedsystems_project1 $i &); done)
cargo build --release && (for i in $(seq 1 6); do (./target/release/weinsa_westem_distributedsystems_project1 $i > log$i &); done)
