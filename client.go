package gossdb

import (
	"github.com/seefan/goerr"
	"github.com/seefan/gopool"
)

const (
	OK       string = "ok"
	NotFound string = "not_found"
)

//可回收的连接，支持连接池。
//非协程安全，多协程请使用多个连接。
type Client struct {
	db     *SSDBClient
	cached *gopool.PooledClient
	pool   *Connectors
}

//关闭连接，连接关闭后只是放回到连接池，不会物理关闭。
func (c *Client) Close() error {
	c.pool.closeClient(c)
	return nil
}

//检查连接情况
//
//  返回 bool，如果可以正常查询数据库信息，就返回true，否则返回false
func (c *Client) Ping() bool {
	_, err := c.Info()
	return err == nil
}

//查询数据库大小
//
//  返回 re，返回数据库的估计大小, 以字节为单位. 如果服务器开启了压缩, 返回压缩后的大小.
//  返回 err，执行的错误
func (c *Client) DbSize() (re int, err error) {
	resp, err := c.db.Do("dbsize")
	if err != nil {
		return -1, err
	}
	if len(resp) == 2 && IsOk(resp[0]) {
		return ToNum(resp[1]), nil
	}
	return -1, makeError(resp)
}
func (c *Client) Do(args ...interface{}) ([][]byte, error) {
	return c.db.Do(args...)
}

//返回服务器的信息.
//
//  返回 re，返回数据库的估计大小, 以字节为单位. 如果服务器开启了压缩, 返回压缩后的大小.
//  返回 err，执行的错误
func (c *Client) Info() (re []string, err error) {
	resp, err := c.db.Do("info")
	if err != nil {
		return nil, err
	}
	if len(resp) > 1 && IsOk(resp[0]) {
		return getResponse(resp[1:]), nil
	}
	return nil, makeError(resp)
}

//生成通过的错误信息，已经确定是有错误
func makeError(resp [][]byte, errKey ...interface{}) error {
	if len(resp) < 1 {
		return goerr.New("ssdb respone error")
	}
	//正常返回的不存在不报错，如果要捕捉这个问题请使用exists
	if IsNotFound(resp[0]) {
		return nil
	}
	if len(errKey) > 0 {
		return goerr.New("access ssdb error, response is %v, parameter is %v", getResponse(resp), errKey)
	} else {
		return goerr.New("access ssdb error, response is %v", getResponse(resp))
	}
}

//判断值是否为ok两个字符
func IsOk(v []byte) bool {
	return len(v) == 2 && v[0] == 'o' && v[1] == 'k'
}
func Is1(v []byte) bool {
	return len(v) == 1 && v[0] == '1'
}

//判断值是否为not_found
func IsNotFound(v []byte) bool {
	return len(v) == 9 && v[0] == 'n' && v[1] == 'o' && v[2] == 't' && v[3] == '_' && v[4] == 'f' && v[5] == 'o' && v[6] == 'u' && v[7] == 'n' && v[8] == 'd'
}
func getResponse(vs [][]byte) (resp []string) {
	for _, v := range vs {
		resp = append(resp, string(v))
	}
	return
}
