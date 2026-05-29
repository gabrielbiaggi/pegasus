import json
d=json.load(open('/tmp/bt.json'))
r=d.get('results',[])
sn=['flat','flat_sl','soros','martingale','mart_sl','fibonacci','pct2','pct2_mart','dalembert']
print('Status:',d['status'],'Dia:',d.get('current_day'),'Dias:',len(r),'/',d.get('total_days'))
print('Elapsed:',int(d.get('elapsed_s',0)),'s')
print()
for x in r:
    st=x.get('strategies',{})
    line=x['date']+' '+str(x.get('total_signals',0)).rjust(3)+'T WR'+str(x.get('signal_wr',0)).rjust(5)+'% '
    for s in sn:
        sr=st.get(s,{})
        b=round(sr.get('balance',0))
        mark = 'X' if sr.get('busted') else ('S' if sr.get('stopped_sl') else '')
        line+=s[:4]+':$'+str(b)+mark+' '
    print(line)
s2=d.get('summary',{}).get('strategies',{})
if s2:
    print()
    print('='*70)
    print('RANKING FINAL:')
    ranked=sorted(s2.items(), key=lambda x: x[1].get('final_balance',0), reverse=True)
    for i,(s,ss) in enumerate(ranked):
        fb=ss.get('final_balance',0)
        tp=ss.get('total_pnl',0)
        roi=ss.get('roi_pct',0)
        bd=ss.get('busted_days',0)
        pd2=ss.get('positive_days',0)
        print(f'  #{i+1} {s:16} Saldo:${fb:>7.2f}  PnL:{tp:>+7.2f}  ROI:{roi:>+6.1f}%  Bust:{bd}  Dias+:{pd2}')
