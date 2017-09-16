import multiprocessing as mp
import numpy as np
import healpy as h
import ctypes as c
import time

def return_values_in_disc(cpixel,discsize,nside):
	v=h.pix2vec(nside,cpixel)
	spixel=h.query_disc(nside,v,discsize*np.pi/180.,inclusive=True,fact=4)
	return spixel

def return_avg_power(cpixel,discsize,nside):
	spixel=return_values_in_disc(cpixel,discsize,nside)
	return np.sum(data[spixel]**2.)/float(size(spixel))

def worker(pix_list,power):
	result=np.zeros(pix_list.size,float)
	for i,x in enumerate(pix_list):
		power[x]=return_avg_power(x,discsize,nside)
		


nside=128 ; lmin=2 ; lmax=3*nside ; discsize=5. # degrees
npix=h.nside2npix(nside)
ell=np.linspace(lmax,lmax,lmax-lmin+1)
cl=ell**-2.5
np.random.seed(0) ; data=h.synfast(cl,nside,verbose=False)

nprocs=16
pchunk=npix/nprocs
pindex=arange(npix)
#print pchunk,pindex
procs=[]

def main():
	power=mp.Array("d",npix)
	procs=[]
	for i in range(nprocs):
		p=mp.Process(target=worker,args=(pindex[i*pchunk:(i+1)*pchunk],power))
		p.start()
		procs=append(procs,p)

	print "Joining back with the master process"
	for p in procs:
		p.join()	

	return np.array(power)

if __name__=="__main__":
	print "Entering main"
	t1=time.time()
	result=main()
	print (time.time()-t1)
	#h.mollview(result)
	#h.mollview(data)



