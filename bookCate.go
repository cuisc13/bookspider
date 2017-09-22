package main

import (
    "os"
    "bufio"
    "fmt"
    "errors"
    "encoding/json"
    "net/url"
    "net/http"
    "strings"
    "bytes"
    "io/ioutil"
    "github.com/robertkrimen/otto"
    "github.com/PuerkitoBio/goquery"
    "github.com/axgle/mahonia"
)
type Result struct{
    AreaName string
    Value interface{}
}

func main(){
    getCate()
}

func ottoTest(){
    jsCmd := `
    var man = {
        name:"Lili"
    }
    console.log('This is a js script')
    console.log(man.name)
    `
    vm := otto.New()
    vm.Run(jsCmd)
    fmt.Println("Test otto js")
    man, _ := vm.Get("man")
    fmt.Println(man)
}

func getCate(){
    for{
        reader := bufio.NewReader(os.Stdin)
        fmt.Print("ISBN: ")
        isbn, _:= reader.ReadString('\n')

        wid, err:= widFromJD(strings.TrimSpace(isbn))
        if err != nil{
            fmt.Println(err)
            continue
        }
        go goQuery(wid.(string))
    }
}

func goQuery(wid string){
    vm := otto.New()
    url := fmt.Sprintf("http://item.jd.com/%s.html", wid)
    doc,err := goquery.NewDocument(url)
    js := doc.Find("head").Find("script").Text()
    if err != nil{
        panic(err)
    }
    spJs := strings.Split(js, "try {")[0]
    gbkBytes := []byte(spJs)
    //用于转码。golang默认编码是utf-8 但是和多网页的编码是gbk，需要将gbk转码称utf-8
    spJs = mahonia.NewDecoder("gbk").ConvertString(string(gbkBytes))
    vm.Run(spJs)
    pageConfig, _ := vm.Get("pageConfig")
    gObject, err := pageConfig.Export()
    switch t := gObject.(type){
    case map[string]interface{}:
        fmt.Println(t["product"].(map[string]interface{})["name"].(string))
        fmt.Println(t["product"].(map[string]interface{})["catName"].([]string))
    default:
        fmt.Println("Unknown type")
    }
}

// 从京东抓取商品链接 读取字节流,返回 以便保存
func widFromJD(isbn string)(interface{}, error){
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
            return nil, errors.New(fmt.Sprintf("从京东获取信息的时候，返回的结果不是合法数据.%v", result.Value))
    }
    wareCount := resValueMap["wareList"].(map[string]interface{})["wareCount"].(float64)
    if wareCount < 1{
        return nil, errors.New(fmt.Sprintf("东京上没能找到该ISBN:%s 的信息", isbn))
    }
    var ware = valueO.(map[string]interface{})["wareList"].(map[string]interface{})["wareList"].([]interface{})[0]
    if ware.(map[string]interface{})["eBookFlag"].(bool){
        ware = valueO.(map[string]interface{})["wareList"].(map[string]interface{})["wareList"].([]interface{})[1]
    }
    wareid := ware.(map[string]interface{})["wareId"].(string)
    //fmt.Println(wareid)
    return wareid, nil
}
