import {expect,test} from 'vitest';import {bytes,savings} from './api';
test('empty storage has no invented savings or ratio',()=>{expect(savings(0,0)).toEqual({bytes:0,ratio:null,percent:0});expect(bytes(0)).toBe('0 B')});
test('deduplication reports measured logical and physical bytes',()=>{expect(savings(2048,1024)).toEqual({bytes:1024,ratio:2,percent:50});expect(bytes(1024)).toBe('1 KiB')});
