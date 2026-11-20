# Independent row-by-row check of the pipeline (different method: plain dicts, one row at a time)
import pandas as pd, sys
D=sys.argv[1]
def rd(k,w): return pd.read_csv(f"{D}/wave{w}_{k}.csv",dtype=str,keep_default_na=False)
BEG,END=pd.Timestamp("1900-01-01"),pd.Timestamp("9999-12-31")
dim=[dict(id=-1,fn="Unknown",ln="Unknown",em="Unknown",city="Unknown",prov="Unknown",tier="Unknown",f=BEG,t=END,cur=True,inf=False)]
fact={}; wm={"c":None,"o":None}; rej=0; cp={}
for w in (1,2,3):
    c=rd("customers",w); o=rd("orders",w)
    newwm={"c":c.modified_at.max(),"o":o.modified_at.max()}
    if wm["c"]: c=c[c.modified_at>wm["c"]]
    if wm["o"]: o=o[o.modified_at>wm["o"]]
    c=c.replace("",None)
    c=c.assign(city=c.city.fillna("Unknown"),province=c.province.fillna("Unknown"),loyalty_tier=c.loyalty_tier.fillna("Bronze"))
    c=c.drop_duplicates().sort_values("modified_at").drop_duplicates("customer_id",keep="last")
    for r in c.sort_values("modified_at").itertuples():
        cid=int(r.customer_id); rows=[d for d in dim if d["id"]==cid]
        if not rows:
            dim.append(dict(id=cid,fn=r.first_name,ln=r.last_name,em=r.email,city=r.city,prov=r.province,tier=r.loyalty_tier,f=BEG,t=END,cur=True,inf=False)); continue
        cur=[d for d in rows if d["cur"]][0]
        if cur["inf"]:
            cur.update(fn=r.first_name,ln=r.last_name,em=r.email,city=r.city,prov=r.province,tier=r.loyalty_tier,inf=False); continue
        for d in rows: d.update(fn=r.first_name,ln=r.last_name,em=r.email)
        if (cur["city"],cur["prov"],cur["tier"])!=(r.city,r.province,r.loyalty_tier):
            ts=pd.Timestamp(r.modified_at); cur["t"]=ts; cur["cur"]=False
            dim.append(dict(id=cid,fn=r.first_name,ln=r.last_name,em=r.email,city=r.city,prov=r.province,tier=r.loyalty_tier,f=ts,t=END,cur=True,inf=False))
    o=o.drop_duplicates().sort_values("modified_at").drop_duplicates("order_id",keep="last")
    for r in o.sort_values("modified_at").itertuples():
        if r.amount=="": rej+=1; continue
        cid=int(r.customer_id) if r.customer_id!="" else -1
        if not any(d["id"]==cid for d in dim):
            dim.append(dict(id=cid,fn="Unknown",ln="Unknown",em="Unknown",city="Unknown",prov="Unknown",tier="Unknown",f=BEG,t=END,cur=True,inf=True))
        fact[int(r.order_id)]=dict(cid=cid,date=pd.Timestamp(r.order_date),amt=float(r.amount),st=r.status)
    for k in wm: wm[k]=max(x for x in (wm[k],newwm[k]) if x)
    cp[w]=(len(dim),len(fact))
from collections import Counter
cnt=Counter(d["id"] for d in dim)
gold=sum(f["amt"] for f in fact.values() for d in dim if d["id"]==f["cid"] and d["f"]<=f["date"]<d["t"] and d["tier"]=="Gold")
print("Q1-4",cp[1],cp[2]); print("Q5",len(dim),"Q6",sum(d["cur"] for d in dim),"Q7",sum(v>1 for v in cnt.values()),"Q8",sum(v==3 for v in cnt.values()),"Q9",sum(d["inf"] for d in dim))
print("Q10",len(fact),"Q11",round(sum(f["amt"] for f in fact.values()),2),"Q12",sum(f["st"]=="Cancelled" for f in fact.values()),"Q13",round(gold,2),"Q14",sum(f["cid"]==-1 for f in fact.values()),"Q15",rej,"Q16",wm["o"])
