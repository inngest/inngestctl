type BlockProps = {
  title?: string;
  className?: string;
  children: React.ReactNode;
};

export default function Block({ title, children, className = '' }: BlockProps) {
  return (
    <div className={className}>
      {title ? <h2 className="text-basis mb-2 text-base font-medium">{title}</h2> : ''}
      {children}
    </div>
  );
}
