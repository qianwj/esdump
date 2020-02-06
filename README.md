# esdump
dump the elasticsearch data written in Rust.

## build:
1. you must have cargo!

check your cargo version:
```shell
cargo -V
```

2. build esdump
```shell
cd /usr/local
git clone https://github.com/qianwj/esdump.git
cd esdump
cargo build
mv target/debug/esdump .
rm -rf target
```

3. set PATH
```shell
export ES_DUMP=/usr/local/esdump/esdump
```

## Usage:
```shell
esdump -i <your es index name>
```

### all paramters:
| name | desc |
| -- | -- |
| A | your es address, default value: http://localhost:9200/ |
| w | scroll window, default value: 1m |
| s | scroll size, default value: 10000 | 
| p | dump path, the directory where zip file saved. default value: ./esdump |
| q | query paramters |
| U | user, optional, if your es using authorization |
| P | password, optional, if your es using authorization |
| rt | request timeout |
| ct | connection timeout |
| midle | max idle connection per host |


### v0.1.0
- support query
- support zip data and upload to s3
- files name <index>.data

### contribute:
- support csv
- support reindex
