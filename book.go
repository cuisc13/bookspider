package main

import(
    "io"
    "os"
    "fmt"
    "log"
    "bytes"
    "errors"
    "strings"
    "net/url"
    "net/http"
    "io/ioutil"
    "crypto/md5"
    "encoding/json"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "github.com/mediocregopher/radix.v2/redis"
    "github.com/mediocregopher/radix.v2/pubsub"
)

type Result struct{
    AreaName string
    Value interface{}
}


type MongoConf struct{
    Host            string   `json:host`
    Port            string   `json:port`
    User            string   `json:user`
    Passwd          string   `json:passwd`
    Database        string   `json:database`
    Collection      string   `json:collection`
}


func main(){
    DIR := os.Getenv("SPIDER_TOOLS")
    conf := getConfig(DIR + "/conf/dushuwang.json")

    logpath := DIR + "/log/imageSniffer.log"
    logfile,err:=os.OpenFile(logpath, os.O_RDWR|os.O_CREATE|os.O_APPEND,0666)
    if err != nil{
        //
    }
    defer logfile.Close()
    logger:=log.New(logfile,"\n",log.Ldate|log.Ltime|log.Llongfile)

    url := "mongodb://"

    if conf.User != ""{
        url = url + conf.User + ":" + conf.Passwd + "@"
    }
    if conf.Host == ""{
        conf.Host = "localhost"
    }
    url = url + conf.Host
    if conf.Port != ""{
        url = url + ":" + conf.Port
    }
    fmt.Println(url)
    session, err := mgo.Dial(url)
    if err != nil{
        logger.Fatal(err)
    }
    defer session.Close()
    if err != nil{
        fmt.Println(err)
    }
    imagesDb := session.DB("book_images")
    gdfs := imagesDb .GridFS("images")

    r, err := redis.Dial("tcp", "localhost:6379")
    if err != nil{
        logger.Fatal(err)
    }
    r.Cmd("SELECT", "1")
    sc := pubsub.NewSubClient(r)
    _ = sc.Subscribe("book.img.NO_IMG_ISBN")


    for {
        /*
        // 测试redis的sub功能
        */
        resp := sc.Receive()
        isbn := resp.Message
        //continue

        /*
        //标准输入获取ISBN
        reader := bufio.NewReader(os.Stdin)
        fmt.Print("ISBN: ")
        isbn, err := reader.ReadString('\n')
        */

        // 通过spop 获得ISBN
        //isbn,err := r.Cmd("SPOP", "NO_IMG_ISBN").Str()

        // 坚持MongoDB 连接是否还有效
        // 如果连接已经能断开  即session.Ping() err 不为nil，则从新建立链接
        err = session.Ping()
        if err != nil{
            logger.Println("MongoDB连接断开，重新建立连接.")
            session,err  = mgo.Dial(url)
            if err != nil{
                logger.Fatal("MongoDB连接建立失败")
            }
            imagesDb = session.DB("book_images")
            gdfs = imagesDb .GridFS("images")
        }

        isbn = strings.TrimSpace(isbn)
        go crawlPic(isbn, gdfs, logger)
        // crawlPic(isbn, gdfs, logger)
    }
}

// 抓取图片并保存在GridFS
func crawlPic(isbn string, gdfs *mgo.GridFS, logger *log.Logger){
    content, err := picFromJD(strings.TrimSpace(isbn), logger)
    if err != nil{
        logger.Printf("从东京获取图片的时候，出错.%v", err)
        return
    }
    n,err := saveGFS(content, gdfs, isbn, logger)
    if err != nil{
        logger.Printf("保存图片的时候，出错.%v", err)
        return
    }
    logger.Printf("写入新文件，ISBN:%s, 长度：%d\n", isbn, n)
}

// 从京东抓取图片 读取字节流,返回 以便保存
func picFromJD(isbn string, logger *log.Logger)([]byte, error){
    urls := "https://so.m.jd.com/ware/searchList.action"
    resp, _:= http.PostForm(urls, url.Values{"_format_":{"json"}, "keyword":{isbn}})
    defer resp.Body.Close()
    content, _ := ioutil.ReadAll(resp.Body)
    var result Result
    dec := json.NewDecoder(bytes.NewReader(content))
    err := dec.Decode(&result)
    if err != nil{
        //fmt.Println(err)
        return nil, err
    }
    //['wareList']['wareList'][0]['wname']
    var valueO interface{}
    dec = json.NewDecoder(strings.NewReader(result.Value.(string)))
    err = dec.Decode(&valueO)
    var resValueMap map[string]interface{}
    switch resValue := valueO.(type){
        case map[string]interface{}:
            resValueMap = resValue
        default:
            return nil, fmt.Errorf("从京东获取图片的时候，返回的结果不是合法数据.%v", result.Value)
    }
    wareCount := resValueMap["wareList"].(map[string]interface{})["wareCount"].(float64)
    if wareCount < 1{
        return nil, fmt.Errorf("东京上没能找到该ISBN:%s 的图片", isbn)
    }
    imageurl := valueO.(map[string]interface{})["wareList"].(map[string]interface{})["wareList"].([]interface{})[0].(map[string]interface{})["imageurl"].(string)
    imageurl = strings.Replace(imageurl, "357x357", "1000x1000", 1)
    //fmt.Println(imageurl)
    logger.Println(imageurl)
    imgresp, _ := http.Get(imageurl)
    defer imgresp.Body.Close()
    content,_ = ioutil.ReadAll(imgresp.Body)
    return content,nil
}

// 保存图片到gridfs中 接受content 字节流 返回保存成功
func saveGFS(content []byte, gdfs *mgo.GridFS, isbn string, logger *log.Logger)(int,error){
    h := md5.New()
    io.WriteString(h, isbn + isbn)
    cryName := fmt.Sprintf("%x",h.Sum(nil))

    if l,_ := gdfs.Find(bson.M{"filename":cryName}).Count();l>0{
        return 0, fmt.Errorf("该ISBN:%s 对应的图片已经存在。", isbn)
    }

    gfile, err := gdfs.Create(cryName)
    logger.Println(cryName)
    defer gfile.Close()
    gfile.SetMeta(bson.M{"isbn":isbn})

    n, err := gfile.Write(content)
    return n,err
}

// 获取配置文件
func getConfig(confpath string) *MongoConf{
    conf := new(MongoConf)
    fbyte, err := ioutil.ReadFile(confpath)
    if err != nil{
        //
    }
    err = json.Unmarshal(fbyte, &conf)
    return conf
}


