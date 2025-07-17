package lib

import (
    "context"
    "sync"
    "time"
    uj "github.com/dryark/ujson/go"
)

const (
    REQ_PUT = iota + 1
    REQ_GET
    REQ_DEL
    REQ_SAVE
    REQ_LOAD
    REQ_TBNEW
    REQ_TBDEL
    REQ_GETRAW
    REQ_PUTRAW
)

type Table struct {
    mu      sync.RWMutex
    records [][]byte
    dirty   map[uint16]bool
}

type Db struct {
    mu       sync.RWMutex
    tables   map[string]*Table
    reqChan  chan Req
    ctx      context.Context
    wg       *sync.WaitGroup
    path     *string
    autosave bool
    dirty    bool
}

type Req interface { Kind() uint8 }

type Rep struct {
    Node  uj.JNode
    Val   []byte
    Error string
}

// ******** PUT ********

type ReqPut struct {
    Table string
    Pos   uint16
    Node  uj.JNode
}
func (ReqPut) Kind() uint8 { return REQ_PUT }

func ( db *Db ) handlePUT( req ReqPut ) {
    table := db.tables[ req.Table ]
    if table == nil {
        table = &Table{
            records: make( [][]byte, 0 ),
            dirty:   make( map[uint16]bool ),
        }
        db.tables[req.Table] = table
    }
    
    data := []byte( req.Node.JsonSave() )
    
    table.records[req.Pos] = data
    table.dirty[req.Pos] = true
    db.dirty = true
    //req.Reply <- Rep{}
}

// ******** PUT RAW ********

type ReqPutRaw struct {
    Table string
    Pos   uint16
    Data  []byte
}
func (ReqPutRaw) Kind() uint8 { return REQ_PUTRAW }

func (db *Db) PutRaw( table string, pos uint16, data []byte ) {
    db.reqChan <- ReqPutRaw{
        Table: table,
        Pos: pos,
        Data: data,
    }
}

func ( db *Db ) handlePUTRAW( req ReqPutRaw ) {
    table := db.tables[ req.Table ]
    if table == nil {
        table = &Table{
            records: make( [][]byte, 0 ),
            dirty:   make( map[uint16]bool ),
        }
        db.tables[req.Table] = table
    }
    
    if req.Pos >= uint16( len( table.records ) ) {
        blanks := make( [][]byte, req.Pos + 1 - uint16( len( table.records ) ) )
        table.records = append( table.records, blanks... )
    }
    table.records[req.Pos] = req.Data
    table.dirty[req.Pos] = true
    db.dirty = true
}

// ******** SAVE ********

type ReqSave struct {
    Reply chan Rep
}
func (ReqSave) Kind() uint8 { return REQ_SAVE }

func ( db *Db ) handleSAVE( req ReqSave ) {
    if db.dirty {
        db.save()
        db.dirty = false
    }
    if req.Reply != nil {
        req.Reply <- Rep{}
    }
}

// ******** LOAD ********

type ReqLoad struct {
    Reply chan Rep
}
func (ReqLoad) Kind() uint8 { return REQ_LOAD }

func ( db *Db ) handleLOAD() {
    db.load()
    db.dirty = false
}

// ******** DEL ********

type ReqDel struct {
    Table string
    Pos   uint16
    //Reply chan Rep
}
func (ReqDel) Kind() uint8 { return REQ_DEL }

func ( db *Db ) handleDEL( req ReqDel ) {
    table := db.tables[ req.Table ]
    if table == nil {
        //req.Reply <- Rep{
        //    Error: &"table not found",
        //}
        return
    }
    table.records[req.Pos] = nil
    table.dirty[req.Pos] = true
    db.dirty = true
}

// ******** GET ********

type ReqGet struct {
    Table string
    Pos   uint16
    Reply chan Rep
}
func (ReqGet) Kind() uint8 { return REQ_GET }

func ( db *Db ) handleGET( req ReqGet ) {
    table := db.tables[ req.Table ]
    if table == nil {
        req.Reply <- Rep{
            Error: "table not found",
        }
        return
    }
    var data []byte
    if req.Pos < uint16( len( table.records ) ) {
        data = table.records[req.Pos]
    }
    if data == nil {
        req.Reply <- Rep{
            Error: "record not found",
        }
        return
    }
    
    node, _ := uj.Parse( data )
    req.Reply <- Rep{ Node: node }
}

// ******** GET RAW ********

type ReqGetRaw struct {
    Table string
    Pos   uint16
    Reply chan Rep
}
func (ReqGetRaw) Kind() uint8 { return REQ_GETRAW }

func (db *Db) GetRaw( table string, pos uint16 ) []byte {
    repChan := make( chan Rep )
    db.reqChan <-ReqGetRaw{
        Table: table,
        Pos: pos,
        Reply: repChan,
    }
    rep := <-repChan
    return rep.Val
}

func ( db *Db ) handleGETRAW( req ReqGetRaw ) {
    table := db.tables[ req.Table ]
    if table == nil {
        req.Reply <- Rep{
            Error: "table not found",
        }
        return
    }
    var data []byte
    if req.Pos < uint16( len( table.records ) ) {
        data = table.records[req.Pos]
    }
    if data == nil {
        req.Reply <- Rep{
            Error: "record not found",
        }
        return
    }
    req.Reply <- Rep{ Val: data }
}

// ******** TABLE NEW ********

type ReqTbNew struct {
    Table string
    Reply chan Rep
}
func (ReqTbNew) Kind() uint8 {
    return REQ_TBNEW
}

func NewDb( path *string, ctx context.Context, wg *sync.WaitGroup, autosave bool ) *Db {
    reqChan := make( chan Req )
    db := &Db{
        path: path,
        tables: make( map[string]*Table ),
        reqChan: reqChan,
        ctx: ctx,
        wg: wg,
        autosave: autosave,
    }
    if path != nil {
        db.load()
    }
    if path != nil && autosave {
        go db.saveLoop()
    }
    go db.loop()
    return db
}

func ( db *Db ) saveLoop() {
    ticker := time.NewTicker( 1 * time.Second )
    defer ticker.Stop()
    
    if db.wg != nil {
        db.wg.Add(1)
    }
    done := db.ctx.Done()
    for {
        select {
            case <-done:
                replyChan := make( chan Rep )
                db.reqChan <- ReqSave{
                    Reply: replyChan,
                }
                <-replyChan
                if db.wg != nil {
                    db.wg.Done()
                }
                return
            case <-ticker.C:
                db.reqChan <- ReqSave{}
        }
    }
}

func ( db *Db ) loop() {
    done := db.ctx.Done()
    for {
        select {
            case <-done:
                return
            case greq := <-db.reqChan:
                switch greq.Kind() {
                    //case REQ_TBNEW: db.handleTBNEW( greq.(ReqTbNew) )
                    case REQ_GET:    db.handleGET(    greq.(ReqGet)    )
                    case REQ_GETRAW: db.handleGETRAW( greq.(ReqGetRaw) )
                    case REQ_PUT:    db.handlePUT(    greq.(ReqPut)    )
                    case REQ_PUTRAW: db.handlePUTRAW( greq.(ReqPutRaw) )
                    case REQ_DEL:    db.handleDEL(    greq.(ReqDel)    )
                    case REQ_SAVE:   db.handleSAVE(   greq.(ReqSave)   )
                    case REQ_LOAD:   db.handleLOAD()
                }
        }
    }
}