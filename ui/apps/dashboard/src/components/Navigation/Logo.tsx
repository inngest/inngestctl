'use client';

import type { Route } from 'next';
import Link from 'next/link';
import { RiContractLeftLine, RiContractRightLine } from '@remixicon/react';

import { toggleNav } from '@/app/actions';
import InngestLogo from '@/icons/InngestLogo';
import { InngestLogoSmall } from '@/icons/InngestLogoSmall';
import Search from './Search';

type LogoProps = {
  collapsed: boolean;
  setCollapsed: (arg: boolean) => void;
};

const NavToggle = ({ collapsed, setCollapsed }: LogoProps) =>
  collapsed ? (
    <RiContractRightLine
      className="bg-canvasBase text-subtle invisible h-5 w-5 cursor-pointer group-hover:visible"
      onClick={() => {
        setCollapsed(!collapsed);
        toggleNav();
      }}
    />
  ) : (
    <RiContractLeftLine
      className="bg-canvasBase text-subtle invisible h-5 w-5 cursor-pointer group-hover:visible"
      onClick={() => {
        setCollapsed(!collapsed);
        toggleNav();
      }}
    />
  );

export default function Logo({ collapsed, setCollapsed }: LogoProps) {
  return (
    <div className="group ml-5 mr-4 mt-5 flex h-10 flex-row items-center justify-between">
      <div className="flex flex-row items-center justify-start">
        {collapsed ? (
          <div className="cursor-pointer group-hover:hidden">
            <InngestLogoSmall />
          </div>
        ) : (
          <>
            <Link href={process.env.NEXT_PUBLIC_HOME_PATH as Route}>
              <InngestLogo className="text-basis mr-3" width={92} />
            </Link>
            <Search />
          </>
        )}
      </div>
      <NavToggle collapsed={collapsed} setCollapsed={setCollapsed} />
    </div>
  );
}