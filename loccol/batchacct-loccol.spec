# sitelib for noarch packages, sitearch for others (remove the unneeded one)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           batchacct-loccol
Version:        1.1
Release:        2%{?dist}
Summary:        Batch Accounting - Local Collection

Group:          Development/Languages
License:        ASL 2.0
URL:            http://cern.ch
Source0:        batchacct/batchacct-loccol-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch:      noarch
Requires:       python-inotify cx_Oracle python-pylsf batchacct-common

%description


%prep
%setup -q


%build
%{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT

 
%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%{python_sitelib}/*
%{_sysconfdir}/init.d/batchacctd
%{_sysconfdir}/cron.d/batchacct-partition.cron
%doc


%post
chkconfig --add batchacctd


%preun
chkconfig --del batchacctd


%changelog
