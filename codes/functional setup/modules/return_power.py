import multiprocessing as mp
import healpy as h 
import numpy as np


# This is an attempt to stop the notebook from crashing
# https://github.com/ipython/ipython/issues/6109
#multiprocessing.set_start_method('spawn')

# Functions to set up
def init_param(x,y):
	global nside, discsize, npix, pindex
	nside=x
	discsize=y
	npix=h.nside2npix(nside)
	pindex=np.arange(npix)

def gen_data(seed=0):
	global data #,lmin,lmax
	lmin=2 ; lmax=3*nside
	ell=np.linspace(lmin,lmax,lmax-lmin+1)
	cl=ell**-2.5
	np.random.seed(seed) ; data=h.synfast(cl,nside,verbose=False)

# Core function which carry out the actual computation.
def return_pixels_in_disc(cpixel):
	v=h.pix2vec(nside,cpixel)
	spixel=h.query_disc(nside,v,discsize*np.pi/180.,inclusive=True,fact=4)
	return spixel

def return_avg_power(cpixel):
	spixel=return_pixels_in_disc(cpixel)
	return np.sum(data[spixel]**2.)/float(np.size(spixel))

def worker(pix_list,sd):
	for x in pix_list:
		sd[x]=return_avg_power(x)

# Serial implementation scheme
def return_power_serial():
	power=np.zeros(npix)
	for i in pindex:
		power[i]=return_avg_power(i)
	return power

# Parallel implementation schemes
def return_power_mppool(numprocs):
	p=mp.Pool(processes=numprocs)
	power=np.array(p.map(return_avg_power,pindex))
	return power

def return_power_pr_shrdmem(numprocs):
	pchunk=npix/numprocs	
	sdpower=mp.Array("d",npix,lock=False)	
	procs=[]
	for i in range(numprocs):
		p=mp.Process(target=worker,args=(pindex[i*pchunk:(i+1)*pchunk],sdpower))
		p.start()
		procs=np.append(procs,p)

	for p in procs:
		p.join()

	return np.array(sdpower[:])
