### Basic steps

- according to server info, set hosts using private IP address
- set `configs/hadoop/workers` and `configs/spark/slaves` 
- copy configs, test, and three shell scripts to $HOME
- `sudo chmod a+x *.sh`
- run `init.sh` firstly
- `source ~/.bashrc`
- after updating bash env and run `init.sh` on all nodes
  - run `nopasswdlog.sh` on each nodes
- run `start.sh` on master01 firstly
  - run `start.sh` on other nodes to start spark