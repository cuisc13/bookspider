package main

import (
    "fmt"
    "github.com/PuerkitoBio/goquery"
    "strings"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/pubsub"
    "encoding/json"
    "io/ioutil"
    "io"
    "net/http"
    "time"
    "log"
    "os"
    "strconv"
    "crypto/md5"
)

var DIR = "/home/workspace/tools"

type Book struct{
    Title,Pic,Price,Href string
    Author, Publish, Series string
    Tag, Isbn,Pubdate string
    Size, Pages string
    Booksummary, Authorsummary string
    Binding, Catalogue, Words string
}

type JUser struct{
    Uname,Uid string
}

type MongoConf struct{
    Host            string   `json:host`
    Port            string   `json:port`
    User            string   `json:user`
    Passwd          string   `json:passwd`
    Database        string   `json:database`
    Collection      string   `json:collection`
}


func pages(i int, url string, col *mgo.Collection, logger *log.Logger){
    //for n := 1; n<10;n++{
    n := 0
    for {
        s := fmt.Sprintf(url, i, n)
        n = n + 1
        doc, err := goquery.NewDocument(s)
        bookinfo := doc.Find("div.list-block.media-list")
        if err != nil{
            break
        }
        alist := bookinfo.Find("a.item-link.item-content")
        alen := alist.Length()
        fmt.Println(alen)
        if alen < 1{
            break
        }
        alist.Each(func(i int, s *goquery.Selection){
            href, exists := s.Attr("href")
            if exists{
                go books(href, col, logger)
            }
            //hrefs = append(hrefs, href)
        })
        if err != nil{
            fmt.Println("Err not nil.")
        }
    }
}

func cates(col *mgo.Collection, logger *log.Logger){
    host := "http://m.dushu.com"
    url := host + "/book/100%d_%d.html"
    //var hrefs []string
    for i := 1;i<10;i++{
        go pages(i, url, col, logger)
    }
    //return hrefs

}

// 直接按照图书ID递增 进行抓取。 现在www 限制为100页， m 端 限制为 10页 没法正常抓取了 只能递增。
func justBooks(col *mgo.Collection, logger *log.Logger, r *redis.Client){
    begins, err := r.Cmd("GET", "dushubookid").Str()
    if err != nil{
        begins = string(13327329)
    }
    begin,_ := strconv.Atoi(begins)
    if  begin<13327329{
        begin = 13327329
    }
    //     10000001 11890000
    //end := 13327329
    //end := 11890000 // 逆序爬
    end := true
    for poi := begin; end; poi ++{
        href := fmt.Sprintf("/book/%d", poi)
        isbn, ok := books(href, col, logger)
        if ok{
            r.Cmd("SET", "dushubookid", poi)
            r.Cmd("SADD","waitimg", isbn)
            r.Cmd("PUBLISH", "book.img.NO_IMG_ISBN_DUSHU", isbn)
        }
        end = ok
    }
}


func books(bookurl string, col *mgo.Collection, logger *log.Logger)(string,bool){
    host := "http://www.dushu.com"
    //for _,bookurl := range books{
        url := host + bookurl
        timeout := time.Duration(5 * time.Second)
        client := http.Client{
            Timeout:timeout,
        }
        resp, err := client.Get(url)
        //fmt.Println(url)
        if resp.StatusCode != 200{
            return "",false
        }
        if err != nil{
            return "",false
        }
        defer resp.Body.Close()
        doc,err := goquery.NewDocumentFromResponse(resp)
        if err != nil{
            //panic(err)
            return "",false
        }

        title:= doc.Find("div.book-title").Text()
        pic,_ := doc.Find("div.pic").Find("img").Attr("src")

        detailsdiv := doc.Find("div.book-details")
        price := detailsdiv.Find("p.price").Find("span").Text()
        var fields []string
        detailsdiv.Find("tr").Each(func(i int, s *goquery.Selection){
            if i<4{
                str := strings.Split(s.Find("td").Text(), "：")[1]
                //str := s.Find("a").Text()
                fields = append(fields, str)
                //fmt.Println(str)
            }else{
                s.Find("td.rt").Each(func(n int, ss *goquery.Selection){
                    str := ss.Text()
                    fields = append(fields, str)
                    //fmt.Println(str)
                })
            }
        })
        doc.Find("div.text.txtsummary").Each(func(i int, s *goquery.Selection){
            fields = append(fields, s.Text())
        })
        if len(fields) < 13{
            return fields[4],true
        }

        b := new(Book)
        b.Title = title
        b.Pic = pic
        b.Href = bookurl
        b.Price = price
        b.Author = fields[0]
        b.Publish = fields[1]
        b.Series = fields[2]
        b.Tag = fields[3]
        b.Isbn = fields[4]
        b.Pubdate = fields[5]
        b.Binding= fields[6]
        b.Size = fields[7]
        b.Pages = fields[8]
        b.Words = fields[9]
        b.Booksummary = fields[10]
        b.Authorsummary = fields[11]
        b.Catalogue = fields[12]
        //fmt.Println(b)
        logger.Println(b.Isbn)
        saveBook(col, b)
        return b.Isbn, true
    //}
}


func execMongo(conf *MongoConf,logger *log.Logger){
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
    //url := fmt.Sprintf("mongodb://%s:%s@%s:%s", conf.User, conf.Passwd, conf.Host, conf.Port)
    fmt.Println(url)
    //session, err := mgo.Dial("mongodb://localhost:7866")
    session, err := mgo.Dial(url)
    if err != nil{
        logger.Println(err)
    }
    defer session.Close()
    //c := session.DB(conf.Database).C(conf.Collection)
    c := session.DB("books").C("book")

    /*
    // 用以抓取图书 并将新抓取的图书isbn保存在redis中，以便以后抓取图片
    r, err := redis.Dial("tcp", "localhost:6379")
    justBooks(c, logger,r)
    // 抓新书 并 保存isbn 结束
    */



    //cates(c, logger, wg)

    //用来抓取图片
    /*
    */
    //getHrefUrl(c, gdfs, logger)
    getHrefUrl(c, session, url, logger)
    //用来抓取图片 结束


}


func saveBook(col *mgo.Collection, book *Book){
    n, err := col.Find(bson.M{"isbn":book.Isbn}).Count()
    if n< 1{
        err = col.Insert(book)
        if err != nil{
         //
        }
    }
}

func getConfig(confpath string) *MongoConf{
    conf := new(MongoConf)
    fbyte, err := ioutil.ReadFile(confpath)
    if err != nil{
        //
    }
    err = json.Unmarshal(fbyte, &conf)
    return conf
}


// 抓取图片 保存到mongodb中的GridFs
func getPic(url , href string, logger *log.Logger)([]byte,bool){
    host := "http://www.dushu.com"
    timeout := time.Duration(5 * time.Second)
    client := http.Client{
        Timeout:timeout,
    }
    req, err := http.NewRequest("GET", url, nil)
    req.Header.Add("Referer", host+href)
    resp, err := client.Do(req)

    if err != nil{
        logger.Println(err)
        return nil, false
    }
    defer resp.Body.Close()
    content, _ := ioutil.ReadAll(resp.Body)
    return content, true

}

func getHrefUrl(col *mgo.Collection, session *mgo.Session, url string, logger *log.Logger){

    imagesDb := session.DB("book_images")
    gdfs := imagesDb .GridFS("images")

    r, err := redis.Dial("tcp", "localhost:6379")
    if err != nil{
        logger.Fatal("Redis connect fail")
    }
    sc := pubsub.NewSubClient(r)
    topic := "book.img.NO_IMG_ISBN_DUSHU"
    _ = sc.Subscribe(topic)
    for{
        // 阻塞等待
        subMsg := sc.Receive()
        resp := subMsg.Message

        /*
        // 轮训从redis中获取。
        //resp, err := r.Cmd("SPOP", "waitimg").Str()
        if err != nil{
            logger.Printf("没有等待处理的isbn了.error: %v\n", err)
            continue
        }
        */
        // 可能会阻塞等待很长时间 这段时间内 MongoDB 的连接可能会被关闭
        // session.Ping()方法 能够检测连接是否还在连接。
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

        h := md5.New()
        io.WriteString(h, resp + resp)
        cryName := fmt.Sprintf("%x",h.Sum(nil))

        if l,_ := gdfs.Find(bson.M{"filename":cryName}).Count();l>0{
            //return 0, errors.New(fmt.Sprintf("该ISBN:%s 对应的图片已经存在。", isbn))
            logger.Printf("该ISBN:%s 对应的图片已经存在。\n", resp)
            continue
        }

        var result map[string]string
        err  = col.Find(bson.M{"isbn":resp}).One(&result)
        logger.Println(err)
        isbn    :=  result["isbn"]
        pic     :=  result["pic"]
        href    :=  result["href"]
        ind     :=  strings.LastIndex(pic, "/")
        rune_pic := []rune(pic)
        picName := string(rune_pic[ind+1:])

        if l,_ := gdfs.Find(bson.M{"filename":cryName}).Count();l>0{
            _ = col.Update(bson.M{"isbn":isbn}, bson.M{"$set":bson.M{"pic":"done"}})
            continue
        }

        gfile, err := gdfs.Create(cryName) //TODO:别忘记关闭gfile
        gfile.SetMeta(bson.M{"name":picName, "isbn":isbn})
        content,ok := getPic(pic, href, logger)
        if !ok{
            _ = gfile.Close()
            continue
        }
        n, err := gfile.Write(content)
        if err != nil{
            logger.Println(err)
        }
        err  = col.Update(bson.M{"isbn":isbn}, bson.M{"$set":bson.M{"pic":"done"}})
        if err != nil{
            //
        }
        logger.Printf("写入新文件，FileName: %v, ISBN:%s, 长度：%d\n", cryName, isbn, n)
        _ = gfile.Close()

    }
}



func main(){
    DIR := os.Getenv("SPIDER_TOOLS")
    logpath := DIR + "/log/dushuwang.log"
    logfile,err:=os.OpenFile(logpath, os.O_RDWR|os.O_CREATE|os.O_APPEND,0666)
    if err != nil{
        //
    }
    defer logfile.Close()
    logger:=log.New(logfile,"\n",log.Ldate|log.Ltime|log.Llongfile)

    //bookurls := pages()
    //pages()
    //books(bookurls)
    //testMongo()
    logger.Println("开始新的爬虫旅程。")
    logger.Println("开始时间: ", time.Now().Unix())
    conf := getConfig(DIR + "/conf/dushuwang.json")
    //url := fmt.Sprintf("mongodb://%s:%s@%s:%s database:%s, collection:%s ", conf.User, conf.Passwd, conf.Host, conf.Port, conf.Database, conf.Collection)
    //fmt.Println(url)
    //logger.Println(url)

    execMongo(conf, logger)
    //testJsonFile()
    //testLog(logpath)

}

func testLog(logpath string){
    logfile,err:=os.OpenFile(logpath, os.O_RDWR|os.O_CREATE|os.O_APPEND,0666)
    if err != nil{
        //
    }
    defer logfile.Close()
    logger:=log.New(logfile,"\n",log.Ldate|log.Ltime|log.Llongfile)
    logger.Println("Wandful!")
    logger.Fatal("胡说!")
}

func testJsonFile(){
    type Human struct{
        Name, Addr string
        Age int
    }
    jso := new(Human)
    //jso := make(map[string]interface{})
    filename := "./test/test.json"
    fbytes, err := ioutil.ReadFile(filename)
    if err != nil{
        //
    }
    err = json.Unmarshal(fbytes, jso)
    fmt.Println(jso.Name, jso.Age, jso.Addr)
}

func testMongo(){
    session, err := mgo.Dial("mongodb://localhost:7866")
    if err != nil{
        //
    }
    defer session.Close()
    c := session.DB("books").C("book")
    //result := JUser{}
    //err = c.Find(bson.M{"uname":"无戒"}).One(&result)
    n, err := c.Find(bson.M{}).Count()
    fmt.Println(n)
}
