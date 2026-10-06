"use strict";
// 不持久化凭证，不把接口内容作为HTML插入页面。
(() => {
  const $ = id => document.getElementById(id);
  let key = "", cursor = null, controller = null;
  const notice = value => { $("notice").textContent = value; };
  const auth = () => ({Authorization: "Bearer " + key});
  async function api(path) {
    const sessionKey = key;
    const response = await fetch(path, {headers:auth(),cache:"no-store"}), data = await response.json();
    if (sessionKey !== key) throw new Error("应用会话已变更，请重新查询");
    if (!response.ok) throw new Error(data.detail || data.error?.code || "请求失败"); return data;
  }
  function option(select, value, label = value) {
    const item = document.createElement("option"); item.value = value; item.textContent = label; select.append(item);
  }
  async function stats() {
    const data = await api("/stats"); $("total").textContent = data.total_requests;
    $("success").textContent = data.groups.filter(x=>x.status==="succeeded").reduce((n,x)=>n+x.requests,0);
    $("other").textContent = data.groups.filter(x=>["failed","cancelled","rejected","abandoned"].includes(x.status)).reduce((n,x)=>n+x.requests,0);
    $("tokens").textContent = data.groups.reduce((n,x)=>n+x.observed_total_tokens,0);
  }
  async function records(next = false) {
    const query = new URLSearchParams({limit:"12"});
    if ($("status").value) query.set("status",$("status").value);
    if ($("filter-model").value) query.set("model",$("filter-model").value);
    if (next && cursor) query.set("before",cursor);
    const data = await api("/requests?"+query); $("rows").replaceChildren(); cursor = data.next_cursor; $("more").disabled = !cursor;
    $("detail").textContent = "点击记录查看错误码、终态和 usage。";
    for (const item of data.items) {
      const row = document.createElement("tr");
      row.dataset.requestId = item.id;
      for (const value of [new Date(item.started_at*1000).toLocaleTimeString(),item.model,item.status,item.first_ms===null?"—":item.first_ms+" ms"]) {
        const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
      }
      row.addEventListener("click",()=>api("/requests/"+item.id).then(data=>{$("detail").textContent=JSON.stringify(data,null,2);}).catch(e=>notice(e.message)));
      $("rows").append(row);
    }
  }
  async function refresh() {
    if (!key) return; try { await Promise.all([stats(),records()]); } catch(e) { notice(e.message); }
  }
  $("login").addEventListener("submit",async event=>{
    event.preventDefault(); if(controller){notice("请先停止当前生成再切换应用");return;} key=$("key").value.trim(); $("key").value="";
    try {
      const data=await api("/v1/models"); $("model").replaceChildren(); $("filter-model").replaceChildren(); option($("filter-model"),"","全部模型");
      for(const model of data.data){option($("model"),model.id);option($("filter-model"),model.id);}
      $("start").disabled=!data.data.length; notice("已连接，记录按当前应用隔离。"); await refresh();
    } catch(e){key="";$("start").disabled=true;notice(e.message);}
  });
  $("logout").addEventListener("click",()=>{
    controller?.abort();key="";cursor=null;$("key").value="";
    for(const id of ["rows","model","filter-model"])$(id).replaceChildren();
    for(const id of ["answer","extra","request-id","detail"])$(id).textContent="";
    for(const id of ["total","success","other","tokens"])$(id).textContent="—";
    $("start").disabled=true;$("more").disabled=true;notice("已退出，凭证已清除。");
  });
  $("refresh").addEventListener("click",refresh);
  for(const id of ["status","filter-model"])$(id).addEventListener("change",refresh);
  $("more").addEventListener("click",()=>records(true).catch(e=>notice(e.message)));
  $("stop").addEventListener("click",()=>controller?.abort());
  $("generate").addEventListener("submit",async event=>{
    event.preventDefault();if(!key||controller)return;controller=new AbortController();$("start").disabled=true;$("stop").disabled=false;
    for(const id of ["answer","extra","request-id"])$(id).textContent="";$("state").textContent="正在等待响应…";
    let reader;
    try {
      const response=await fetch("/v1/chat/completions",{method:"POST",headers:{...auth(),"Content-Type":"application/json"},signal:controller.signal,
        body:JSON.stringify({model:$("model").value,messages:[{role:"user",content:$("prompt").value}],stream:true})});
      $("request-id").textContent=response.headers.get("X-Request-ID")||"";
      if(!response.ok){const data=await response.json();throw new Error(data.detail||data.error?.code||"请求失败");}
      reader=response.body.getReader();const decoder=new TextDecoder();let buffer="",done=false;
      while(!done){
        const part=await reader.read();if(part.done)break;buffer+=decoder.decode(part.value,{stream:true});buffer=buffer.replace(/\r\n/g,"\n");let position;
        while((position=buffer.indexOf("\n\n"))!==-1){
          const frame=buffer.slice(0,position);buffer=buffer.slice(position+2);
          const data=frame.split("\n").filter(x=>x.startsWith("data:")).map(x=>x.slice(5).trimStart()).join("\n");
          if(!data)continue;if(data==="[DONE]"){done=true;break;}
          const value=JSON.parse(data);if(value.error)throw new Error(value.error.code||"上游错误");
          for(const choice of value.choices||[]){const delta=choice.delta||{};
            if(delta.content)$("answer").textContent+=delta.content;
            if(delta.reasoning_content)$("extra").textContent+=delta.reasoning_content;
            if(delta.tool_calls)$("extra").textContent+=JSON.stringify(delta.tool_calls)+"\n";
          }$("state").textContent="正在生成…";
        }
      }if(!done)throw new Error("响应中断，未收到成功结束标记");$("state").textContent="已完成";
    }catch(e){$("state").textContent=e.name==="AbortError"?"已停止，保留部分输出":"失败："+e.message;}
    finally{try{await reader?.cancel();}catch{}$("stop").disabled=true;await refresh();controller=null;$("start").disabled=!key;}
  });
  fetch("/health").then(r=>{$("health").textContent=r.ok?"依赖就绪":"依赖异常";}).catch(()=>{$("health").textContent="服务不可达";});
})();
