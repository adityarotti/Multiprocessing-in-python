import multiprocessing as mp
import numpy as np
import healpy as h
import time

def return_values_in_disc(cpixel):
	v=h.pix2vec(nside,cpixel)
	spixel=h.query_disc(nside,v,discsize*np.pi/180.,inclusive=True,fact=4)
	return spixel

def return_avg_power(cpixel):
	spixel=return_values_in_disc(cpixel)
	return np.sum(data[spixel]**2.)/float(size(spixel))

nside=256 ; lmin=2 ; lmax=3*nside ; discsize=5. # degrees
npix=h.nside2npix(nside)
ell=np.linspace(lmax,lmax,lmax-lmin+1)
cl=ell**-2.5
np.random.seed(0) ; data=h.synfast(cl,nside,verbose=False)
pindex=np.arange(npix)

if __name__=="__main__":
	t1=time.time()
	p=mp.Pool(processes=16)
	result=np.array(p.map(return_avg_power,pindex))
	print (time.time()-t1)
	#h.mollview(result)
	#h.mollview(data)
	
