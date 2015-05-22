#!/bin/bash
APP_ROOT='/home/ubuntu/tempus_integration/OpenAds-Flask/'
sudo add-apt-repository "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/"
sudo add-apt-repository ppa:opencpu/opencpu-1.4
sudo apt-get update
sudo apt-get install r-base r-base-dev littler
sudo r install_r_packages.r
# Haven't tested this command
sudo apt-get install opencpu
sudo apt-get install rstudio-server #optional
sudo service apache2 restart
cd '$APP_ROOT'
sudo make install_gz

