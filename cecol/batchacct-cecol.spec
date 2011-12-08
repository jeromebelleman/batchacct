# sitelib for noarch packages, sitearch for others (remove the unneeded one)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           batchacct-cecol
Version:        1.1
Release:        1%{?dist}
Summary:        Batch Accounting - CE Collection

Group:          Development/Languages
License:        ASL 2.0
URL:            http://cern.ch
Source0:        batchacct/batchacct-cecol-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch:      noarch
Requires:       python-inotify cx_Oracle batchacct-common

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
%config(noreplace) %{_sysconfdir}/batchacct
%{_sysconfdir}/init.d/batchacct-cecold
%doc


%post
chkconfig --add batchacct-cecold


%preun
chkconfig --del batchacct-cecold


%changelog
