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


nside=256 ; lmin=2 ; lmax=3*nside ; discsize=5. # degrees
ell=np.linspace(lmax,lmax,lmax-lmin+1)
cl=ell**-2.5
np.random.seed(0) ; data=h.synfast(cl,nside)
power=zeros(data.size,float)

def main():
	for i in range(data.size):
		power[i]=return_avg_power(i,discsize,nside)
	return power

if __name__=="__main__":
	t1=time.time()
	result=main()
	print (time.time()-t1)
	h.mollview(result)
	h.mollview(data)




