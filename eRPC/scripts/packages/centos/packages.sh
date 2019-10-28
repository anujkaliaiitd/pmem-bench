#!/bin/bash
#
# This script sets up a fresh CentOS-7 box for eRPC.
#
# For best perforamance, Mellanox OFED should be installed by downloading from
# Mellanox. However, eRPC should work with upstream mlx* packages as well

# Update to the latest CentOS (e.g., CentOS 7.4 -> CentOS 7.5)
sudo yum -y update

# This is required for yum to find most packages below
wget -P /tmp/ https://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/e/epel-release-7-11.noarch.rpm
sudo rpm -Uvh /tmp/epel-release*rpm

###
### Packages required for eRPC
###
sudo yum -y install gcc-c++ cmake numactl-devel numactl bc gflags-devel \
  boost-devel gtest gtest-devel libpmem libpmem-devel dpdk dpdk-devel

###
### Packages required to install Mellanox OFED
###
sudo yum -y install createrepo python2-devel elfutils-libelf-devel \
  redhat-rpm-config rpm-build libxml2-python gtk2 \
  gcc-gfortran atk tk tcl tcsh perl

###
### Optional convenience packages
###

# Update vim
sudo curl -L https://copr.fedorainfracloud.org/coprs/mcepl/vim8/repo/epel-7/mcepl-vim8-epel-7.repo -o /etc/yum.repos.d/mcepl-vim8-epel-7.repo
sudo yum -y update vim*

# General packages not specific to eRPC
sudo yum -y install htop memcached libmemcached-devel ctags-etags \
  the_silver_searcher sloccount calc

# Fuzzy find configuration
git clone --depth 1 https://github.com/junegunn/fzf.git ~/.fzf
yes | ~/.fzf/install

# Vundle
git clone https://github.com/VundleVim/Vundle.vim.git ~/.vim/bundle/Vundle.vim
