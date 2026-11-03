package model
import "testing"
func TestManifestChecksum(t *testing.T){m:=Manifest{Version:1,ID:"one",Files:[]File{}};b,h,e:=m.Encode();if e!=nil{t.Fatal(e)};if _,e=Decode(b,h);e!=nil{t.Fatal(e)};b[0]='x';if _,e=Decode(b,h);e==nil{t.Fatal("corruption accepted")}}
