module github.com/yougroupteam/kaf

go 1.12

require (
	github.com/Landoop/schema-registry v0.0.0-20190327143759-50a5701c1891
	github.com/Shopify/sarama v1.23.1
	github.com/avast/retry-go v2.4.1+incompatible
	github.com/birdayz/kaf v0.1.22
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/protobuf v1.3.2
	github.com/hokaccha/go-prettyjson v0.0.0-20190818114111-108c894c2c0e
	github.com/jhump/protoreflect v1.5.0
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/magiconair/properties v1.8.1
	github.com/manifoldco/promptui v0.3.2
	github.com/mattn/go-colorable v0.1.2
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cobra v0.0.5
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/birdayz/kaf v0.1.22 => github.com/yougroupteam/kaf v0.1.23-0.20190909102058-ae084a0233b3
