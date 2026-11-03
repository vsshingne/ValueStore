package safepath

import (
 "fmt"
 "io/fs"
 "os"
 "path/filepath"
 "strings"
)
// Relative rejects Windows drive/ADS/device forms on every platform.
func Relative(p string)error{if p=="" || !fs.ValidPath(p) || strings.ContainsAny(p,"\\:\x00") {return fmt.Errorf("invalid relative path")};for _,part:=range strings.Split(p,"/"){base:=strings.ToUpper(strings.Split(part,".")[0]);if strings.HasSuffix(part,".") || strings.HasSuffix(part," ") || base=="CON" || base=="PRN" || base=="AUX" || base=="NUL" || (len(base)==4 && (strings.HasPrefix(base,"COM") || strings.HasPrefix(base,"LPT")) && base[3]>='0' && base[3]<='9'){return fmt.Errorf("reserved path")}};return nil}
// Root uses Go's traversal-resistant filesystem API. Symlinks are also rejected by scans.
func Root(base,rel string)(*os.Root,error){if e:=Relative(rel);e!=nil{return nil,e};r,e:=os.OpenRoot(base);if e!=nil{return nil,e};defer r.Close();return r.OpenRoot(filepath.FromSlash(rel))}
