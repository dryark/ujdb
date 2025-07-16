package main

import (
    "context"
    "fmt"
    udb "github.com/dryark/ujdb"
)

func main() {
    ctx, cancel := context.WithCancel( context.Background() )
    
    db := udb.NewDb( nil, ctx, nil, false )
    
    db.PutRaw( "test", 0, []byte("blah") )
    
    val := string( db.GetRaw( "test", 0 ) )
    
    fmt.Printf("val:%s\n", val )
    
    cancel()
}