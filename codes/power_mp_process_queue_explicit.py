import multiprocessing as mp
import numpy as np
import healpy as h
import time

def return_values_in_disc(cpixel,discsize,nside):
	v=h.pix2vec(nside,cpixel)
	spixel=h.query_disc(nside,v,discsize*np.pi/180.,inclusive=True,fact=4)
	return spixel

def return_avg_power(cpixel,discsize,nside):
	spixel=return_values_in_disc(cpixel,discsize,nside)
	return np.sum(data[spixel]**2.)/float(size(spixel))

def worker(pix_list,out_q):
	result=np.zeros(pix_list.size,float)
	for i,x in enumerate(pix_list):
		result[i]=return_avg_power(x,discsize,nside)
	out_q.put(result)
		


nside=256 ; lmin=2 ; lmax=3*nside ; discsize=5. # degrees
ell=np.linspace(lmin,lmax,lmax-lmin+1)
cl=ell**-2.5
np.random.seed(0) ; data=h.synfast(cl,nside,verbose=False)
power=zeros(data.size,float)

nprocs=8
pchunk=h.nside2npix(nside)/nprocs
pindex=arange(h.nside2npix(nside))
#print pchunk,pindex
procs=[]

def main():
	out_q1=mp.Queue()
	out_q2=mp.Queue()
	out_q3=mp.Queue()
	out_q4=mp.Queue()
	out_q5=mp.Queue()
	out_q6=mp.Queue()
	out_q7=mp.Queue()
	out_q8=mp.Queue()

	p1=mp.Process(target=worker,args=(pindex[0*pchunk:pchunk],out_q1))
	p2=mp.Process(target=worker,args=(pindex[pchunk:2*pchunk],out_q2))
	p3=mp.Process(target=worker,args=(pindex[2*pchunk:3*pchunk],out_q3))
	p4=mp.Process(target=worker,args=(pindex[3*pchunk:4*pchunk],out_q4))
	p5=mp.Process(target=worker,args=(pindex[4*pchunk:5*pchunk],out_q5))
	p6=mp.Process(target=worker,args=(pindex[5*pchunk:6*pchunk],out_q6))
	p7=mp.Process(target=worker,args=(pindex[6*pchunk:7*pchunk],out_q7))
	p8=mp.Process(target=worker,args=(pindex[7*pchunk:8*pchunk],out_q8))
	
	print "Started processes"
	p1.start()
	p2.start()
	p3.start()
	p4.start()
	p5.start()
	p6.start()
	p7.start()
	p8.start()
	
	print "Getting results"
	result=[]
	result=append(result,out_q1.get())
	result=append(result,out_q2.get())
	result=append(result,out_q3.get())
	result=append(result,out_q4.get())
	result=append(result,out_q5.get())
	result=append(result,out_q6.get())
	result=append(result,out_q7.get())
	result=append(result,out_q8.get())
	print "Joining back with the master process"
	
	p1.join()
	p2.join()
	p3.join()
	p4.join()
	p5.join()
	p6.join()
	p7.join()
	p8.join()
	

	#result=r1+r2

	return result

if __name__=="__main__":
	print "Entering main"
	t1=time.time()
	result=main()
	print (time.time()-t1)
	#h.mollview(result)
	#h.mollview(data)



