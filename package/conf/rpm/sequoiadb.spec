Summary: SequoiaDB
Name: sequoiadb
Version: SDB_ENGINE_VERISON_CURRENT.SDB_ENGINE_SUBVERSION_CURRENT
Release: 1
License: AGPL
Source:sequoiadb-SDB_ENGINE_VERISON_CURRENT.SDB_ENGINE_SUBVERSION_CURRENT.tar.gz
Group: Applications/Databases
AutoReqProv: no
%define InstallPath /opt/sequoiadb
%description
NoSQL database.
%prep
%setup
%build
%install
echo 3 > /proc/sys/net/ipv4/tcp_retries2
mkdir -p $RPM_BUILD_ROOT%{InstallPath}
cp -rf * $RPM_BUILD_ROOT%{InstallPath}
%clean
rm -rf $RPM_BUILD_ROOT
rm -rf $RPM_BUILD_DIR/%{name}-%{version}
%post
groupadd sdbadmin_group
useradd sdbadmin -p sdbadmin -d %{InstallPath} -g sdbadmin_group -s /bin/bash
chown sdbadmin:sdbadmin_group -R %{InstallPath}
echo "NAME=sdbcm" > /etc/default/sequoiadb
echo "SDBADMIN_USER=sdbadmin" >> /etc/default/sequoiadb
echo "INSTALL_DIR=/opt/sequoiadb" >> /etc/default/sequoiadb
cp -f %{InstallPath}/sequoiadb /etc/init.d/sdbcm
chmod +x /etc/init.d/sdbcm
chkconfig --add sdbcm
/etc/init.d/sdbcm start
%preun
service sdbcm stop
%{InstallPath}/bin/sdbstop
chkconfig --del sdbcm
rm -rf /etc/init.d/sdbcm
%postun
rm -rf /etc/default/sequoiadb
%files
%defattr(-,root,root)
/opt/sequoiadb/
%changelog
* Wed Aug 6 2014 lijianhua <lijianhua@sequoiadb.com>
- First draft of the spec file
