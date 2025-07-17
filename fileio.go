package lib

import (
    "bufio"
    "encoding/binary"
    "errors"
    "io"
    "os"
)

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