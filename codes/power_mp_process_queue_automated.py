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
		


nside=64 ; lmin=2 ; lmax=3*nside ; discsize=5. # degrees
ell=np.linspace(lmax,lmax,lmax-lmin+1)
cl=ell**-2.5
np.random.seed(0) ; data=h.synfast(cl,nside,verbose=False)
power=zeros(data.size,float)

nprocs=8
pchunk=h.nside2npix(nside)/nprocs
pindex=arange(h.nside2npix(nside))
#print pchunk,pindex
procs=[]

def main():
	out_q=mp.Queue()

	procs=[]
	for i in range(nprocs):
		p=mp.Process(target=worker,args=(pindex[i*pchunk:(i+1)*pchunk],out_q))
		p.start()
		procs=append(procs,p)

	print "Getting results"
	print out_q.get()

	print "Joining back with the master process"
	for p in procs:
		p.join()	

	return result

if __name__=="__main__":
	print "Entering main"
	t1=time.time()
	result=main()
	print (time.time()-t1)
	#h.mollview(result)
	#h.mollview(data)



