const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const source=fs.readFileSync(process.env.WISHLIST_SYNC_SOURCE || 'backups/index_sincronizacion_pendiente.html','utf8');
const adapter=source.slice(source.indexOf('        var zsSnapshot'),source.indexOf('        window.onload = async function()'));
function setup(){
 const disk=new Map(); let fail=false,hook=null; const sent=[];
 const c={console:{error(){}},TextEncoder,crypto:require('node:crypto').webcrypto,setTimeout:()=>0,clearTimeout(){},setInterval(){},Map,Set,Promise,Error,JSON,Date,Math};
 Object.assign(c,{db:{a:[{_sid:'i',src:'old',timestamp:1}]},pDb:{},pTeamCrests:{},structure:[{groupId:'g',collections:[{id:'a',name:'A'}]}],pStructure:[],pOrders:{sheets:[{id:'s',name:'S',rows:[{id:'r',name:'old'}]}],activeSheetId:'s'},pedidosArchivo:[],pedidosClientes:['C'],pedidosProveedores:['P'],pedidoVersionOptions:['FAN'],savedAddresses:[{id:'ad',label:'Casa'}],pedidosPreciosClientes:{C:{fan:10}},localStorage:{getItem(){return null},setItem(){},removeItem(){}},localforage:{async setItem(k,v){if(hook){const h=hook;hook=null;h()} if(fail){fail=false;throw Error('disk')}disk.set(k,JSON.parse(JSON.stringify(v)))},async getItem(k){return disk.get(k)}},ZSync:{user:{id:'u'},put(c,id,d){sent.push([c,id,d])},del(c,id){sent.push([c,id,null])}}});c.window=c;
 for(const n of ['init','pInit','renderOrderSheetsBar','renderOrderTable','renderClientesChips','renderProveedoresChips','renderPedidosTab','renderMorosos','saveDBQuiet','pSaveDBQuiet','savePedidosArchivo'])c[n]=()=>{};
 vm.createContext(c);vm.runInContext(adapter,c);c.zsReady=true;c.zsSnapshotUser='u';c.zsSnapshot=Object.fromEntries([...c.zsBuildRecords().records].map(([k,r])=>[k,r.json]));
 c.zsDownload=async()=>{};c.zsEnsurePaths=async()=>{};
 return {c,disk,sent,fail(){fail=true},hook(fn){hook=fn}};
}
const change=(name)=>[{collection:'sheet_row',id:'s__r',data:{id:'r',parent:'s',order:0,name}}];
test('granularidad, orden y borrado',async()=>{const {c,sent}=setup();assert.ok(c.zsSnapshot['address/ad']);assert.ok(c.zsSnapshot['proveedor/P']);assert.ok(c.zsSnapshot['sheet_row/s__r']);assert.equal(JSON.parse(c.zsSnapshot['sheet/s']).rows,undefined);await c.zsOnChange([{collection:'proveedor',id:'P',deleted:true}]);assert.equal(c.pedidosProveedores.length,0); c.db.a=[];await c.zsDiffInner();assert.ok(sent.some(x=>x[0]==='wl_item'&&x[2]===null));});
test('rechazo descarga conserva checkpoint y reintenta',async()=>{const {c}=setup();const before=JSON.stringify(c.zsSnapshot);c.zsDownload=async()=>{throw Error('download')};await assert.rejects(c.zsOnChange(change('remote')));assert.equal(JSON.stringify(c.zsSnapshot),before);c.zsDownload=async()=>{};await c.zsOnChange(change('remote'));assert.equal(c.pOrders.sheets[0].rows[0].name,'remote');});
test('rechazo escritura conserva memoria/checkpoint y reintenta',async()=>{const {c,fail}=setup();const before=JSON.stringify(c.zsSnapshot);fail();await assert.rejects(c.zsOnChange(change('remote')));assert.equal(JSON.stringify(c.zsSnapshot),before);assert.equal(c.pOrders.sheets[0].rows[0].name,'old');await c.zsOnChange(change('remote'));assert.equal(c.pOrders.sheets[0].rows[0].name,'remote');});
test('edición durante descarga se conserva y luego sube',async()=>{const {c,sent}=setup();c.zsDownload=async()=>{c.pOrders.sheets[0].rows[0].name='local';c.zsTouch()};await c.zsOnChange(change('remote'));assert.equal(c.pOrders.sheets[0].rows[0].name,'local');await c.zsDiffInner();assert.ok(sent.some(x=>x[0]==='sheet_row'&&x[2].name==='local'));});
test('edición durante persistencia rechaza sin pisar edición',async()=>{const {c,hook}=setup();hook(()=>{c.pOrders.sheets[0].rows[0].name='local';c.zsTouch()});await assert.rejects(c.zsOnChange(change('remote')));assert.equal(c.pOrders.sheets[0].rows[0].name,'local');await c.zsOnChange(change('remote'));assert.equal(c.pOrders.sheets[0].rows[0].name,'local');});
test('foto pendiente tras fallo de subida sobrevive recepción',async()=>{const {c,sent}=setup();c.db.a[0].src='data:image/jpeg;base64,LOCAL';c.zsTouch();await c.zsDiffInner();assert.ok(!sent.some(x=>x[0]==='wl_item'));await c.zsOnChange([{collection:'wl_item',id:'i',data:{_sid:'i',src:'remote',col:'a',timestamp:1}}]);assert.equal(c.db.a[0].src,'data:image/jpeg;base64,LOCAL');c.zsSrcToPath.set(c.db.a[0].src,'img/local');await c.zsDiffInner();assert.ok(sent.some(x=>x[0]==='wl_item'&&x[2].src.$img==='img/local'));});

test("copia previa verificable y descarga completas",async()=>{
 const {c,disk}=setup(); c.Blob=Blob;c.buildFullBackup=()=>({__backup:"wishlist-full-v1",biblioteca:{pTeamCrests:{a:"crest"}},pedidos:{savedAddresses:[{label:"Casa"}]}});let downloaded;
 c.saveFileUniversal=async(name,blob)=>{downloaded=JSON.parse(await blob.text());return true};
 await c.zsBackupBeforeFirstSync();assert.equal(downloaded.biblioteca.pTeamCrests.a,"crest");assert.equal(disk.get("zs_wishlist_pre_sync_backup_v1").pedidos.savedAddresses[0].label,"Casa");
});
test("copia previa aborta ante escritura, verificacion o descarga fallida",async()=>{
 const {c,fail}=setup();c.Blob=Blob;c.buildFullBackup=()=>({a:1});c.saveFileUniversal=async()=>true;
 fail();await assert.rejects(c.zsBackupBeforeFirstSync(),/disk/);
 const get=c.localforage.getItem;c.localforage.getItem=async()=>({a:2});await assert.rejects(c.zsBackupBeforeFirstSync(),/verificar/);c.localforage.getItem=get;
 c.saveFileUniversal=async()=>false;await assert.rejects(c.zsBackupBeforeFirstSync(),/descargar/);
 c.saveFileUniversal=async()=>{throw Error("download")};await assert.rejects(c.zsBackupBeforeFirstSync(),/download/);
});
