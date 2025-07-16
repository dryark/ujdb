package lib

import (
    "bufio"
    "context"
    "encoding/binary"
    "errors"
    "io"
    //"fmt"
    "os"
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

type ReqPut struct {
    Table string
    Pos   uint16
    Node  uj.JNode
    //Reply ->chan Rep
}
func (ReqPut) Kind() uint8 {
    return REQ_PUT
}

type ReqPutRaw struct {
    Table string
    Pos   uint16
    Data  []byte
    //Reply ->chan Rep
}
func (ReqPutRaw) Kind() uint8 {
    return REQ_PUTRAW
}
func (db *Db) PutRaw( table string, pos uint16, data []byte ) {
    db.reqChan <- ReqPutRaw{
        Table: table,
        Pos: pos,
        Data: data,
    }
}

type Req interface {
    Kind() uint8
}

type ReqSave struct {
    Reply chan Rep
}
func (ReqSave) Kind() uint8 {
    return REQ_SAVE
}

type ReqLoad struct {
    Reply chan Rep
}
func (ReqLoad) Kind() uint8 {
    return REQ_LOAD
}

type ReqDel struct {
    Table string
    Pos   uint16
    //Reply chan Rep
}
func (ReqDel) Kind() uint8 {
    return REQ_DEL
}

type ReqGet struct {
    Table string
    Pos   uint16
    Reply chan Rep
}
func (ReqGet) Kind() uint8 {
    return REQ_GET
}

type ReqGetRaw struct {
    Table string
    Pos   uint16
    Reply chan Rep
}
func (ReqGetRaw) Kind() uint8 {
    return REQ_GETRAW
}
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

type ReqTbNew struct {
    Table string
    Reply chan Rep
}
func (ReqTbNew) Kind() uint8 {
    return REQ_TBNEW
}

type Rep struct {
    Node  uj.JNode
    Val   []byte
    Error string
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
                    //case REQ_TBNEW:
                    //    req := greq.(ReqTbNew)
                    
                    case REQ_GET:
                        req := greq.(ReqGet)
                        table := db.tables[ req.Table ]
                        if table == nil {
                            req.Reply <- Rep{
                                Error: "table not found",
                            }
                            continue
                        }
                        var data []byte
                        if req.Pos < uint16( len( table.records ) ) {
                            data = table.records[req.Pos]
                        }
                        if data == nil {
                            req.Reply <- Rep{
                                Error: "record not found",
                            }
                            continue
                        }
                        
                        node, _ := uj.Parse( data )
                        req.Reply <- Rep{ Node: node }
                    
                    case REQ_GETRAW:
                        req := greq.(ReqGetRaw)
                        table := db.tables[ req.Table ]
                        if table == nil {
                            req.Reply <- Rep{
                                Error: "table not found",
                            }
                            continue
                        }
                        var data []byte
                        if req.Pos < uint16( len( table.records ) ) {
                            data = table.records[req.Pos]
                        }
                        if data == nil {
                            req.Reply <- Rep{
                                Error: "record not found",
                            }
                            continue
                        }
                        req.Reply <- Rep{ Val: data }
                        
                    case REQ_PUT:
                        req := greq.(ReqPut)
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
                    
                    case REQ_PUTRAW:
                        req := greq.(ReqPutRaw)
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
                    
                    case REQ_DEL:
                        req := greq.(ReqDel)
                        table := db.tables[ req.Table ]
                        if table == nil {
                            //req.Reply <- Rep{
                            //    Error: &"table not found",
                            //}
                            continue
                        }
                        table.records[req.Pos] = nil
                        table.dirty[req.Pos] = true
                        db.dirty = true
                        
                    case REQ_SAVE:
                        req := greq.(ReqSave)
                        if db.dirty {
                            db.save()
                            db.dirty = false
                        }
                        if req.Reply != nil {
                            req.Reply <- Rep{}
                        }
                        
                    case REQ_LOAD:
                        //req := greq.(ReqLoad)
                        db.load()
                        db.dirty = false
                        
                }
        }
    }
}

func ( db *Db ) save() error {
    file, err := os.Create( *db.path )
    if err != nil {
        return err
    }
    defer file.Close()
    
    w := bufio.NewWriter( file )
    
    for name, table := range db.tables {
        if len( name ) > 255 {
            return errors.New("table name too long")
        }
        w.Write( []byte("TB") )
        w.WriteByte( uint8( len( name ) ) )
        w.WriteString( name )
        
        for _, rec := range table.records {
            if len( rec ) > 65535 {
                return errors.New("record too long")
            }
            w.Write( []byte("R1") )
            binary.Write( w, binary.BigEndian, uint16( len( rec ) ) )
            w.Write( rec )
        }
        
        // Todo: Set dirty map to clean
    }
    
    return w.Flush()
}

func ( db *Db ) load() error {
    file, err := os.Open( *db.path )
    if err != nil {
        return err
    }
    defer file.Close()
    
    //db := &Db{
    //    tables: make(map[string]*Table),
    //}
    
    r := bufio.NewReader(file)
    var curTable *Table
    
    for {
        marker := make( []byte, 2 )
        _, err := io.ReadFull( r, marker )
        if err == io.EOF {
            break
        } else if err != nil {
            return err
        }
        
        switch string(marker) {
            case "TB":
                nameLenByte, err := r.ReadByte()
                if err != nil {
                    return err
                }
                nameLen := int( nameLenByte )
                name := make( []byte, nameLen )
                if _, err := io.ReadFull( r, name ); err != nil {
                    return err
                }
                tbl := &Table{
                    records: make( [][]byte, 0 ),
                    dirty:   make( map[uint16]bool ),
                }
                db.tables[ string( name ) ] = tbl
                curTable = tbl
                
            case "R1":
                if curTable == nil {
                    return errors.New("record encountered before table header")
                }
                var length uint16
                if err := binary.Read( r, binary.BigEndian, &length ); err != nil {
                    return err
                }
                data := make( []byte, length )
                if _, err := io.ReadFull( r, data ); err != nil {
                    return err
                }
                curTable.records = append( curTable.records, data )
                
            default:
                return errors.New( "unknown marker: " + string( marker ) )
        }
    }
    
    return nil
}